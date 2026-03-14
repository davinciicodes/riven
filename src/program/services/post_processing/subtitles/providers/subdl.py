"""
Subdl subtitle provider for Riven.
"""

import io
import zipfile

import httpx
from babelfish import Language, Error as BabelfishError
from loguru import logger

from .base import SubtitleItem, SubtitleProvider


SEARCH_URL = "https://api.subdl.com/api/v1/subtitles"
DOWNLOAD_BASE = "https://dl.subdl.com"


def _to_subdl_language(language: str) -> str | None:
    """Convert ISO 639-3 code to Subdl's uppercase ISO 639-1 code (e.g. 'eng' → 'EN')."""
    try:
        return Language(language.strip().lower()).alpha2.upper()
    except (BabelfishError, ValueError, AttributeError):
        return None


class SubdlProvider(SubtitleProvider):
    """
    Subdl subtitle provider.

    Uses the Subdl REST API (https://subdl.com) which searches by IMDB ID.
    Requires a free API key from https://subdl.com/user/api.
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("Subdl API key is required")
        self.api_key = api_key

    @property
    def name(self) -> str:
        return "subdl"

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
        subdl_lang = _to_subdl_language(language)

        if not subdl_lang:
            logger.debug(f"Subdl: unsupported language code '{language}', skipping")
            return []

        if not imdb_id and not filename:
            logger.debug("Subdl: no IMDB ID or filename, skipping")
            return []

        params: dict[str, str | int] = {
            "api_key": self.api_key,
            "subs_per_page": 30,
            "languages": subdl_lang,
        }

        if imdb_id:
            params["imdb_id"] = imdb_id  # Subdl accepts tt-prefixed IMDB IDs
        else:
            params["film_name"] = filename  # type: ignore[assignment]

        if season is not None:
            params["season_number"] = season

        if episode is not None:
            params["episode_number"] = episode

        try:
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(SEARCH_URL, params=params)
                resp.raise_for_status()
                data = resp.json()

            if not data.get("status"):
                logger.debug(f"Subdl: search returned status=false for {imdb_id or filename}")
                return []

            subtitles = data.get("subtitles") or []

            if not subtitles:
                logger.debug(f"Subdl: no results for {imdb_id or filename} [{subdl_lang}]")
                return []

            results: list[SubtitleItem] = []

            for sub in subtitles:
                url = sub.get("url")
                if not url:
                    continue

                # Skip full-season packs when searching for a single episode
                if episode is not None and sub.get("full_season"):
                    continue

                release_name = sub.get("release_name") or sub.get("name") or "subtitle.srt"
                matched_by = "imdbid" if imdb_id else "query"

                results.append(
                    SubtitleItem(
                        id=str(sub.get("sd_id", "")),
                        language=language,
                        filename=release_name,
                        download_count=0,
                        rating=0.0,
                        matched_by=matched_by,
                        movie_hash=None,
                        movie_name=sub.get("name"),
                        provider=self.name,
                        score=self._calculate_score(sub, bool(imdb_id), filename),
                        download_url=DOWNLOAD_BASE + url,
                    )
                )

            logger.debug(
                f"Subdl: found {len(results)} subtitle(s) for {imdb_id or filename} [{subdl_lang}]"
            )
            return results

        except httpx.HTTPStatusError as e:
            logger.error(f"Subdl search HTTP error {e.response.status_code}: {e}")
        except Exception as e:
            logger.error(f"Subdl search error: {e}")

        return []

    def download_subtitle(self, subtitle_info: SubtitleItem) -> str | None:
        url = subtitle_info.download_url

        if not url:
            logger.warning(f"Subdl: no download URL for subtitle {subtitle_info.id}")
            return None

        try:
            with httpx.Client(timeout=30, follow_redirects=True) as client:
                resp = client.get(url)
                resp.raise_for_status()
                zip_bytes = resp.content

            content = self._extract_srt_from_zip(zip_bytes)

            if content:
                logger.debug(f"Subdl: downloaded subtitle '{subtitle_info.filename}'")

            return content

        except httpx.HTTPStatusError as e:
            logger.error(f"Subdl download HTTP error {e.response.status_code}: {e}")
        except Exception as e:
            logger.error(f"Subdl download error: {e}")

        return None

    def _extract_srt_from_zip(self, zip_bytes: bytes) -> str | None:
        """Extract the first SRT file from a ZIP archive."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                srt_names = [n for n in zf.namelist() if n.lower().endswith(".srt")]

                if not srt_names:
                    logger.debug("Subdl: ZIP contains no .srt files")
                    return None

                # Prefer the first SRT file
                with zf.open(srt_names[0]) as f:
                    raw = f.read()

            return self._decode_content(raw)

        except zipfile.BadZipFile:
            logger.error("Subdl: downloaded file is not a valid ZIP")
            return None

    def _decode_content(self, raw: bytes) -> str | None:
        """Decode subtitle bytes with encoding fallbacks."""
        for encoding in ("utf-8", "utf-8-sig", "iso-8859-1", "windows-1252"):
            try:
                return raw.decode(encoding)
            except (UnicodeDecodeError, UnicodeError):
                continue
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            logger.error("Subdl: failed to decode subtitle content")
            return None

    def _calculate_score(
        self,
        sub: dict,
        is_imdb_match: bool,
        filename: str | None,
    ) -> float:
        score: float = 2500.0 if is_imdb_match else 1000.0

        # Prefer hearing-impaired subs slightly lower (optional: adjust if desired)
        if sub.get("hi"):
            score -= 50

        # Boost if release_name resembles our filename (crude but useful)
        release_name = (sub.get("release_name") or "").lower()
        if filename and release_name:
            fn = filename.lower()
            # Count shared tokens as a quick similarity measure
            fn_tokens = set(fn.replace(".", " ").replace("_", " ").split())
            rn_tokens = set(release_name.replace(".", " ").replace("_", " ").split())
            shared = len(fn_tokens & rn_tokens)
            score += shared * 10

        return score
