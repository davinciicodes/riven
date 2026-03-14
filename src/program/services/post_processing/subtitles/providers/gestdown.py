"""
Gestdown provider for Riven.

REST API wrapper around Addic7ed subtitles — TV shows only.
Addic7ed subtitles are manually synced by fans, giving superior timing
accuracy for current airing shows compared to hash-based providers.

No API key required. Docs: https://gestdown.readme.io
"""

import re

import httpx
from loguru import logger

from .base import SubtitleItem, SubtitleProvider


BASE_URL = "https://api.gestdown.info"

# ISO 639-3 → Gestdown/Addic7ed full language names
_LANGUAGE_NAMES: dict[str, str] = {
    "eng": "English",
    "fra": "French",
    "spa": "Spanish",
    "deu": "German",
    "ita": "Italian",
    "por": "Portuguese",
    "nld": "Dutch",
    "pol": "Polish",
    "rus": "Russian",
    "swe": "Swedish",
    "nor": "Norwegian",
    "dan": "Danish",
    "fin": "Finnish",
    "ces": "Czech",
    "slk": "Slovak",
    "hun": "Hungarian",
    "ron": "Romanian",
    "bul": "Bulgarian",
    "hrv": "Croatian",
    "srp": "Serbian",
    "slv": "Slovenian",
    "tur": "Turkish",
    "ell": "Greek",
    "heb": "Hebrew",
    "ara": "Arabic",
    "zho": "Chinese",
    "jpn": "Japanese",
    "kor": "Korean",
    "cat": "Catalan",
    "ukr": "Ukrainian",
    "vie": "Vietnamese",
    "ind": "Indonesian",
}


def _parse_show_title(filename: str) -> str | None:
    """Extract show title from an episode filename (e.g. 'Show.Title.S01E01.mkv' → 'Show Title')."""
    # Remove file extension
    name = re.sub(r"\.[^.]+$", "", filename)
    # Find S##E## or season/episode markers and take everything before
    match = re.search(r"[Ss]\d{1,2}[Ee]\d{1,2}", name)
    if match:
        title_part = name[: match.start()]
        title = re.sub(r"[._]+", " ", title_part).strip()
        return title or None
    return None


class GestdownProvider(SubtitleProvider):
    """
    Gestdown REST API provider (Addic7ed subtitles).

    TV episodes only — returns empty results for movies.
    Subtitles are manually timed by fans, making them particularly
    accurate for current-airing series.

    Two-step lookup:
    1. Search show by title → get showUniqueId
    2. Fetch subtitles by showUniqueId + season + episode + language
    """

    def __init__(self):
        # Cache show title → showUniqueId to avoid repeated lookups
        self._show_cache: dict[str, str] = {}

    @property
    def name(self) -> str:
        return "gestdown"

    def search_subtitles(
        self,
        imdb_id: str,
        video_hash: str | None = None,
        file_size: int | None = None,
        filename: str | None = None,
        search_tags: str | None = None,
        season: int | None = None,
        episode: int | None = None,
        language: str = "eng",
    ) -> list[SubtitleItem]:
        # Gestdown is TV-only
        if season is None or episode is None:
            return []

        if not filename:
            logger.debug("Gestdown: no filename to parse show title from, skipping")
            return []

        lang_name = _LANGUAGE_NAMES.get(language.lower())
        if not lang_name:
            logger.debug(f"Gestdown: unsupported language '{language}', skipping")
            return []

        show_title = _parse_show_title(filename)
        if not show_title:
            logger.debug(f"Gestdown: could not parse show title from '{filename}'")
            return []

        try:
            show_id = self._lookup_show(show_title)
            if not show_id:
                return []

            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(
                    f"{BASE_URL}/subtitles/get/{show_id}/{season}/{episode}/{lang_name}"
                )

                if resp.status_code == 404:
                    logger.debug(
                        f"Gestdown: no subtitles for '{show_title}' "
                        f"S{season:02d}E{episode:02d} [{lang_name}]"
                    )
                    return []

                if resp.status_code == 423:
                    logger.debug(f"Gestdown: show '{show_title}' is being refreshed, skipping")
                    return []

                resp.raise_for_status()
                data = resp.json()

            subtitles = data.get("matchingSubtitles") or []
            episode_info = data.get("episode") or {}
            show_name = episode_info.get("show_name", show_title)

            results: list[SubtitleItem] = []

            for sub in subtitles:
                download_uri = sub.get("downloadUri")
                if not download_uri:
                    continue

                # Only include completed subtitles
                if not sub.get("completed", True):
                    continue

                version = sub.get("version") or f"S{season:02d}E{episode:02d}"

                results.append(
                    SubtitleItem(
                        id=str(sub.get("subtitleId", "")),
                        language=language,
                        filename=version,
                        download_count=0,
                        rating=0.0,
                        matched_by="imdbid",
                        movie_hash=None,
                        movie_name=show_name,
                        provider=self.name,
                        score=self._calculate_score(sub, filename),
                        download_url=download_uri,
                    )
                )

            logger.debug(
                f"Gestdown: found {len(results)} subtitle(s) for '{show_title}' "
                f"S{season:02d}E{episode:02d} [{lang_name}]"
            )
            return results

        except httpx.HTTPStatusError as e:
            logger.error(f"Gestdown HTTP {e.response.status_code}: {e}")
        except Exception as e:
            logger.error(f"Gestdown search error: {e}")

        return []

    def download_subtitle(self, subtitle_info: SubtitleItem) -> str | None:
        url = subtitle_info.download_url
        if not url:
            return None

        try:
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                resp = client.get(url)
                resp.raise_for_status()
                raw = resp.content

            content = self._decode(raw)
            if content:
                logger.debug(f"Gestdown: downloaded '{subtitle_info.filename}'")
            return content

        except httpx.HTTPStatusError as e:
            logger.error(f"Gestdown download error: HTTP {e.response.status_code}")
        except Exception as e:
            logger.error(f"Gestdown download error: {e}")

        return None

    def _lookup_show(self, title: str) -> str | None:
        """Return showUniqueId for the given title, using cache to avoid redundant calls."""
        cache_key = title.lower().strip()

        if cache_key in self._show_cache:
            return self._show_cache[cache_key]

        try:
            encoded = title.replace(" ", "%20")
            with httpx.Client(timeout=10, follow_redirects=True) as client:
                resp = client.get(f"{BASE_URL}/shows/search/{encoded}")

                if resp.status_code == 404:
                    logger.debug(f"Gestdown: show '{title}' not found")
                    return None

                resp.raise_for_status()
                data = resp.json()

            # Response may be a list of shows or a single show object
            shows = data if isinstance(data, list) else [data]

            if not shows:
                return None

            # Pick the closest name match
            best = _best_show_match(title, shows)
            if not best:
                return None

            show_id = best.get("showUniqueId") or best.get("id")
            if not show_id:
                return None

            self._show_cache[cache_key] = str(show_id)
            logger.debug(f"Gestdown: resolved '{title}' → {show_id}")
            return str(show_id)

        except httpx.HTTPStatusError as e:
            logger.error(f"Gestdown show lookup HTTP {e.response.status_code}: {e}")
        except Exception as e:
            logger.error(f"Gestdown show lookup error: {e}")

        return None

    def _calculate_score(self, sub: dict, filename: str | None) -> float:
        # Manually synced subtitles get a base above IMDB-matched auto-providers
        score = 2800.0

        if sub.get("hearingImpaired"):
            score -= 50.0

        if sub.get("hd"):
            score += 20.0

        if sub.get("corrected"):
            score += 30.0

        # Boost if version string matches release tokens in filename
        version = (sub.get("version") or "").lower()
        if filename and version:
            fn_tokens = set(
                filename.lower().replace(".", " ").replace("_", " ").split()
            )
            v_tokens = set(version.replace(".", " ").replace("_", " ").split())
            score += len(fn_tokens & v_tokens) * 10.0

        return score

    def _decode(self, raw: bytes) -> str | None:
        for enc in ("utf-8", "utf-8-sig", "iso-8859-1", "windows-1252"):
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, UnicodeError):
                continue
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            logger.error("Gestdown: failed to decode subtitle content")
            return None


def _best_show_match(query: str, shows: list[dict]) -> dict | None:
    """Pick the show whose name best matches the query (case-insensitive token overlap)."""
    query_tokens = set(query.lower().split())
    best_score = -1
    best_show = None

    for show in shows:
        name = (show.get("name") or show.get("title") or "").lower()
        name_tokens = set(name.split())
        score = len(query_tokens & name_tokens)
        if score > best_score:
            best_score = score
            best_show = show

    return best_show
