"""
OpenSubtitles REST API provider (opensubtitles.com v2) for Riven.

Distinct from the XML-RPC opensubtitles.org provider — uses the newer
REST API with JSON responses and direct download links.
Requires a free API key from https://www.opensubtitles.com/consumers
Free tier: 5 downloads/day (no login), 20/day (free account).
"""

import gzip

import httpx
from babelfish import Language, Error as BabelfishError
from loguru import logger

from .base import SubtitleItem, SubtitleProvider


BASE_URL = "https://api.opensubtitles.com/api/v1"


def _to_rest_language(language: str) -> str | None:
    """Convert ISO 639-3 code to 2-letter code for the REST API (e.g. 'eng' → 'en')."""
    try:
        return Language(language.strip().lower()).alpha2
    except (BabelfishError, ValueError, AttributeError):
        return None


class OpenSubtitlesRestProvider(SubtitleProvider):
    """
    OpenSubtitles.com REST API v1 provider.

    Searches by IMDB ID. For TV episodes, uses parent_imdb_id + season/episode
    per the API spec. Downloads return direct SRT links (no base64/zlib).
    """

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("OpenSubtitles REST API key is required")
        self._headers = {
            "Api-Key": api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "Riven/1.0",
        }

    @property
    def name(self) -> str:
        return "opensubtitles_rest"

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
        lang_2 = _to_rest_language(language)

        if not lang_2:
            logger.debug(f"OpenSubtitles REST: unsupported language '{language}', skipping")
            return []

        if not imdb_id:
            logger.debug("OpenSubtitles REST: no IMDB ID, skipping")
            return []

        params: dict[str, str | int] = {"languages": lang_2}

        is_episode = season is not None and episode is not None

        if is_episode:
            # For TV episodes, the API wants the series IMDB ID as parent_imdb_id
            params["parent_imdb_id"] = imdb_id
            params["season_number"] = season
            params["episode_number"] = episode
            params["type"] = "episode"
        else:
            params["imdb_id"] = imdb_id
            params["type"] = "movie"

        if video_hash:
            params["moviehash"] = video_hash

        try:
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(
                    f"{BASE_URL}/subtitles",
                    params=params,
                    headers=self._headers,
                )
                resp.raise_for_status()
                data = resp.json()

            items = data.get("data") or []

            if not items:
                logger.debug(
                    f"OpenSubtitles REST: no results for {imdb_id} [{lang_2}]"
                    + (f" S{season:02d}E{episode:02d}" if is_episode else "")
                )
                return []

            results: list[SubtitleItem] = []

            for item in items:
                attrs = item.get("attributes", {})
                files = attrs.get("files") or []

                if not files:
                    continue

                file_id = str(files[0].get("file_id", ""))
                file_name = files[0].get("file_name") or attrs.get("release") or "subtitle.srt"
                feature = attrs.get("feature_details") or {}
                matched_by = "moviehash" if attrs.get("moviehash_match") else "imdbid"

                results.append(
                    SubtitleItem(
                        id=file_id,
                        language=language,
                        filename=file_name,
                        download_count=attrs.get("download_count") or 0,
                        rating=float(attrs.get("ratings") or 0),
                        matched_by=matched_by,
                        movie_hash=None,
                        movie_name=feature.get("movie_name") or feature.get("title"),
                        provider=self.name,
                        score=self._calculate_score(attrs, matched_by),
                    )
                )

            logger.debug(
                f"OpenSubtitles REST: found {len(results)} result(s) for {imdb_id} [{lang_2}]"
            )
            return results

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 401:
                logger.error("OpenSubtitles REST: invalid API key")
            elif status == 429:
                logger.warning("OpenSubtitles REST: rate limited")
            else:
                logger.error(f"OpenSubtitles REST: HTTP {status} on search")
        except Exception as e:
            logger.error(f"OpenSubtitles REST search error: {e}")

        return []

    def download_subtitle(self, subtitle_info: SubtitleItem) -> str | None:
        try:
            file_id = int(subtitle_info.id)

            with httpx.Client(timeout=30, follow_redirects=True) as client:
                # Step 1: request a download link
                dl_resp = client.post(
                    f"{BASE_URL}/download",
                    json={"file_id": file_id},
                    headers=self._headers,
                )

                if dl_resp.status_code == 406:
                    logger.warning(
                        "OpenSubtitles REST: daily download quota exhausted"
                    )
                    return None

                dl_resp.raise_for_status()
                dl_data = dl_resp.json()
                link = dl_data.get("link")

                if not link:
                    return None

                remaining = dl_data.get("remaining", "?")
                logger.debug(
                    f"OpenSubtitles REST: {remaining} download(s) remaining today"
                )

                # Step 2: download the subtitle file
                sub_resp = client.get(link, headers={"User-Agent": "Riven/1.0"})
                sub_resp.raise_for_status()
                raw = sub_resp.content

            # Handle optional gzip encoding
            if raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)

            content = self._decode(raw)

            if content:
                logger.debug(
                    f"OpenSubtitles REST: downloaded '{subtitle_info.filename}'"
                )

            return content

        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 406:
                logger.warning("OpenSubtitles REST: daily quota exhausted")
            elif status == 429:
                logger.warning("OpenSubtitles REST: rate limited on download")
            else:
                logger.error(f"OpenSubtitles REST download error: HTTP {status}")
        except Exception as e:
            logger.error(f"OpenSubtitles REST download error: {e}")

        return None

    def _calculate_score(self, attrs: dict, matched_by: str) -> float:
        score = 10000.0 if matched_by == "moviehash" else 2500.0

        if attrs.get("from_trusted"):
            score += 200.0
        if not attrs.get("ai_translated"):
            score += 100.0
        if not attrs.get("machine_translated"):
            score += 50.0
        if attrs.get("hearing_impaired"):
            score -= 30.0

        # Popularity tiebreaker (up to 100 pts)
        downloads = attrs.get("download_count") or 0
        score += min(downloads // 500, 100)

        # Rating tiebreaker (up to 100 pts)
        score += float(attrs.get("ratings") or 0) * 10

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
            logger.error("OpenSubtitles REST: failed to decode subtitle content")
            return None
