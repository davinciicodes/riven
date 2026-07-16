"""
Base provider interface for subtitle providers.
"""

import time
from abc import ABC, abstractmethod

from pydantic import BaseModel


class SubtitleItem(BaseModel):
    id: str
    language: str
    filename: str
    download_count: int
    rating: float
    matched_by: str
    movie_hash: str | None
    movie_name: str | None
    provider: str
    score: float
    download_url: str | None = None  # Pre-resolved download URL (used by providers like Subdl)


class SubtitleProvider(ABC):
    """Abstract base class for subtitle providers."""

    _cooldown_until: float = 0.0

    def mark_rate_limited(self, cooldown_seconds: int = 3600):
        """Mark this provider as rate-limited for the given duration."""
        self._cooldown_until = time.monotonic() + cooldown_seconds

    @property
    def is_rate_limited(self) -> bool:
        return time.monotonic() < self._cooldown_until

    @abstractmethod
    def search_subtitles(
        self,
        imdb_id: str,
        video_hash: str | None = None,
        file_size: int | None = None,
        filename: str | None = None,
        search_tags: str | None = None,
        season: int | None = None,
        episode: int | None = None,
        language: str = "en",
    ) -> list[SubtitleItem]:
        """
        Search for subtitles.

        Args:
            imdb_id: IMDB ID of the media
            video_hash: OpenSubtitles hash of the video file
            file_size: Size of the video file in bytes
            filename: Original filename (for fallback matching)
            search_tags: Comma-separated tags (release group, format) for OpenSubtitles
            season: Season number (for TV shows)
            episode: Episode number (for TV shows)
            language: ISO 639-3 language code

        Returns:
            list of subtitle results
        """
        pass

    @abstractmethod
    def download_subtitle(self, subtitle_info: SubtitleItem) -> str | None:
        """Download subtitle content."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name."""
        pass
