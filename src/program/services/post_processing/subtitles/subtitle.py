"""
Subtitle service for Riven.

Handles subtitle fetching from various providers and stores them in the database
for serving via RivenVFS.
"""

from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm import object_session
from loguru import logger

from program.db.db import db_session
from program.media.item import Episode, MediaItem, Movie
from program.media.subtitle_entry import SubtitleEntry
from program.settings import settings_manager
from program.settings.models import SubtitleConfig
from program.services.post_processing.subtitles.providers.base import (
    SubtitleItem,
    SubtitleProvider,
)
from program.core.analysis_service import AnalysisService
from .providers.opensubtitles import OpenSubtitlesProvider
from .providers.opensubtitles_rest import OpenSubtitlesRestProvider
from .providers.subdl import SubdlProvider
from .providers.gestdown import GestdownProvider


class SubtitleService(AnalysisService[SubtitleConfig]):
    """Service for fetching and managing subtitles."""

    def __init__(self):
        super().__init__()

        self.settings = settings_manager.settings.post_processing.subtitle
        self.initialized = False

        if not self.settings.enabled:
            logger.debug("Subtitle service is disabled")
            return

        # Initialize providers
        self.providers = list[SubtitleProvider]()
        self._initialize_providers()

        if not self.providers:
            logger.warning("No subtitle providers initialized")
            return

        # Parse language codes
        self.languages = self._parse_languages(self.settings.languages)

        if not self.languages:
            logger.warning("No valid languages configured for subtitles")
            return

        self.initialized = True
        logger.info(
            f"Subtitle service initialized with {len(self.providers)} provider(s) and {len(self.languages)} language(s)"
        )

    @classmethod
    def get_key(cls) -> str:
        return "subtitle"

    def _initialize_providers(self):
        """Initialize configured subtitle providers."""

        provider_configs = self.settings.providers

        # Initialize OpenSubtitles provider
        if provider_configs.opensubtitles.enabled:
            try:
                provider = OpenSubtitlesProvider()
                self.providers.append(provider)
                logger.debug("OpenSubtitles provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize OpenSubtitles provider: {e}")

        # Initialize Subdl provider
        if provider_configs.subdl.enabled:
            try:
                provider = SubdlProvider(api_key=provider_configs.subdl.api_key)
                self.providers.append(provider)
                logger.debug("Subdl provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Subdl provider: {e}")

        # Initialize OpenSubtitles REST provider
        if provider_configs.opensubtitles_rest.enabled:
            try:
                provider = OpenSubtitlesRestProvider(api_key=provider_configs.opensubtitles_rest.api_key)
                self.providers.append(provider)
                logger.debug("OpenSubtitles REST provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize OpenSubtitles REST provider: {e}")

        # Initialize Gestdown provider (Addic7ed, TV shows only)
        if provider_configs.gestdown.enabled:
            try:
                provider = GestdownProvider()
                self.providers.append(provider)
                logger.debug("Gestdown provider initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Gestdown provider: {e}")

    @classmethod
    def _parse_languages(cls, language_codes: list[str]) -> list[str]:
        """
        Parse and validate language codes.

        Args:
            language_codes: list of language codes (ISO 639-1, ISO 639-2, or ISO 639-3)

        Returns:
            list of valid ISO 639-3 language codes
        """

        from .providers.opensubtitles import normalize_language_to_alpha3

        valid_languages = list[str]()

        for lang_code in language_codes:
            try:
                normalized = normalize_language_to_alpha3(lang_code)

                if (
                    normalized
                    and normalized != "eng"
                    or lang_code.lower() in ["en", "eng"]
                ):
                    valid_languages.append(normalized)
                elif normalized == "eng" and lang_code.lower() not in ["en", "eng"]:
                    # Only add 'eng' if it was explicitly requested
                    logger.warning(
                        f"Language code '{lang_code}' normalized to 'eng' (fallback)"
                    )
                else:
                    valid_languages.append(normalized)
            except Exception as e:
                logger.error(f"Failed to parse language code '{lang_code}': {e}")

        return list(set(valid_languages))  # Remove duplicates

    @property
    def enabled(self) -> bool:
        """Check if the subtitle service is enabled."""

        return self.settings.enabled and self.initialized

    def run(self, item: MediaItem) -> bool:
        """
        Fetch and store subtitles for a media item.

        Note: Caller (PostProcessing) is responsible for checking should_submit()
        and ensuring item type is movie/episode.

        Args:
            item: MediaItem to fetch subtitles for (must be movie or episode)
        """

        if not self.enabled:
            logger.debug(f"Subtitle service not enabled, skipping {item.log_string}")
            return False

        if not item.filesystem_entry:
            logger.warning(
                f"No filesystem entry for {item.log_string}, cannot fetch subtitles"
            )
            return False

        try:
            logger.debug(f"Fetching subtitles for {item.log_string}")

            # Get existing embedded subtitles from media_metadata
            embedded_subtitle_languages = self._get_embedded_subtitle_languages(item)

            if embedded_subtitle_languages:
                logger.debug(
                    f"Found {len(embedded_subtitle_languages)} embedded subtitle language(s) in {item.log_string}: {', '.join(embedded_subtitle_languages)}"
                )

            # Get video file information
            # Get VFS paths (use base path for video_path)
            media_entry = item.media_entry

            assert media_entry

            vfs_paths = media_entry.get_all_vfs_paths()

            if not vfs_paths:
                logger.warning(
                    f"No VFS paths for {item.log_string}, cannot fetch subtitles"
                )
                return False

            video_path = vfs_paths[0]  # Use base path
            video_hash = self._calculate_video_hash(item)
            original_filename = media_entry.get_original_filename()

            # Build search tags from media_metadata for better OpenSubtitles matching
            # Tags are release group names and format identifiers (BluRay, HDTV, etc.)
            # NOT full filenames - see https://trac.opensubtitles.org/opensubtitles/wiki/XMLRPC#Supportedtags
            search_tags = self._build_search_tags(item)

            # Get IMDB ID
            imdb_id = item.imdb_id

            # Get season/episode info for TV shows
            season = None
            episode = None

            if isinstance(item, Episode):
                season = item.parent.number
                episode = item.number

            # Get file size
            file_size = item.filesystem_entry.file_size

            # Search for subtitles in each language
            for language in self.languages:
                # Skip if language already exists as embedded subtitle
                if language in embedded_subtitle_languages:
                    logger.debug(
                        f"Skipping {language} subtitle for {item.log_string} - already embedded in video"
                    )
                    continue

                try:
                    self._fetch_subtitle_for_language(
                        item=item,
                        language=language,
                        video_path=video_path,
                        video_hash=video_hash,
                        file_size=file_size,
                        original_filename=original_filename,
                        search_tags=search_tags,
                        imdb_id=imdb_id,
                        season=season,
                        episode=episode,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to fetch {language} subtitle for {item.log_string}: {e}"
                    )

            logger.debug(f"Finished fetching subtitles for {item.log_string}")

            return True
        except Exception as e:
            logger.error(f"Failed to fetch subtitles for {item.log_string}: {e}")

        return False

    @classmethod
    def _get_embedded_subtitle_languages(cls, item: MediaItem) -> set[str]:
        """
        Extract embedded subtitle languages from media_metadata.

        Checks the media_metadata.subtitle_tracks array from MediaAnalysisService
        and returns a set of ISO 639-3 language codes.

        Args:
            item: MediaItem with filesystem_entry containing media_metadata

        Returns:
            Set of ISO 639-3 language codes (e.g., {'eng', 'spa', 'fre'})
        """

        embedded_languages = set[str]()

        try:
            media_entry = item.media_entry

            if not media_entry or not media_entry.media_metadata:
                return embedded_languages

            for track in media_entry.media_metadata.subtitle_tracks:
                lang = track.language

                if lang and lang != "unknown":
                    # Convert to ISO 639-3 if needed
                    # FFprobe typically returns ISO 639-2 (3-letter codes)
                    # which are compatible with our language list
                    embedded_languages.add(lang)

        except Exception as e:
            logger.warning(
                f"Failed to extract embedded subtitle languages for {item.log_string}: {e}"
            )

        return embedded_languages

    def _build_search_tags(self, item: MediaItem) -> str | None:
        """
        Build comma-separated search tags from media_metadata for OpenSubtitles.

        Tags are specific identifiers like release groups (AXXO, KILLERS) and
        format tags (BluRay, HDTV, DVD, etc.), NOT full filenames.

        See: https://trac.opensubtitles.org/opensubtitles/wiki/XMLRPC#Supportedtags

        Args:
            item: MediaItem with media_metadata

        Returns:
            Comma-separated tags string (e.g., "BluRay,ETRG") or None
        """

        tags = list[str]()

        try:
            if not (media_entry := item.media_entry) or not media_entry.media_metadata:
                return None

            # MediaMetadata stores parsed data from RTN at the top level
            # No need to access nested parsed_filename - fields are directly accessible
            from RTN import parse

            # Re-parse the original filename to get release group and quality info
            # This is necessary because MediaMetadata doesn't store all RTN fields
            original_filename = media_entry.get_original_filename()

            if not original_filename:
                return None

            parsed = parse(original_filename)

            if not parsed:
                return None

            # Add release group if available
            release_group = parsed.group

            if release_group:
                tags.append(release_group)

            # Add quality/format tag (BluRay, HDTV, DVD, etc.)
            quality = parsed.quality

            if quality:
                tags.append(quality)

            # Add other relevant tags
            if parsed.proper:
                tags.append("Proper")

            if parsed.repack:
                tags.append("Repack")

            if quality and "remux" in quality.lower():
                tags.append("Remux")

            if parsed.extended:
                tags.append("Extended")

            if parsed.unrated:
                tags.append("Unrated")

            if tags:
                tags_str = ",".join([t.lower() for t in tags])

                logger.debug(f"Built search tags for {item.log_string}: {tags_str}")

                return tags_str

        except Exception as e:
            logger.warning(f"Failed to build search tags for {item.log_string}: {e}")

        return None

    def _calculate_video_hash(self, item: MediaItem) -> str | None:
        """
        Calculate OpenSubtitles hash for the video file.

        Reads the first and last 64KB directly from the CDN URL using a sync
        HTTP client, bypassing the FUSE mount to avoid trio/asyncio conflicts.

        Args:
            item: MediaItem with filesystem entry

        Returns:
            OpenSubtitles hash or None if calculation fails
        """
        import struct
        import httpx

        CHUNK_SIZE = 65536  # 64KB

        try:
            media_entry = item.media_entry
            assert media_entry

            file_size = media_entry.file_size
            if not file_size or file_size < CHUNK_SIZE * 2:
                logger.debug(
                    f"File too small ({file_size} bytes) to calculate hash for {item.log_string}"
                )
                return None

            cdn_url = media_entry.unrestricted_url or media_entry.download_url
            if not cdn_url:
                logger.debug(f"No CDN URL for {item.log_string}, cannot calculate hash")
                return None

            with httpx.Client(timeout=30, follow_redirects=True) as client:
                # First 64KB
                r1 = client.get(cdn_url, headers={"Range": f"bytes=0-{CHUNK_SIZE - 1}"})
                if r1.status_code not in (200, 206):
                    logger.debug(f"CDN returned {r1.status_code} for first chunk of {item.log_string}")
                    return None
                first_chunk = r1.content

                # Last 64KB
                last_start = max(0, file_size - CHUNK_SIZE)
                r2 = client.get(cdn_url, headers={"Range": f"bytes={last_start}-{file_size - 1}"})
                if r2.status_code not in (200, 206):
                    logger.debug(f"CDN returned {r2.status_code} for last chunk of {item.log_string}")
                    return None
                last_chunk = r2.content

            hash_value = file_size
            for chunk in (first_chunk, last_chunk):
                for i in range(0, len(chunk), 8):
                    if i + 8 <= len(chunk):
                        value = struct.unpack("<Q", chunk[i : i + 8])[0]
                        hash_value = (hash_value + value) & 0xFFFFFFFFFFFFFFFF

            video_hash = f"{hash_value:016x}"
            logger.debug(f"Calculated OpenSubtitles hash for {item.log_string}: {video_hash}")
            return video_hash

        except Exception as e:
            logger.error(f"Failed to calculate video hash for {item.log_string}: {e}")
            return None

    def _fetch_subtitle_for_language(
        self,
        item: MediaItem,
        language: str,
        video_path: str,
        video_hash: str | None,
        file_size: int | None,
        original_filename: str,
        search_tags: str | None,
        imdb_id: str | None,
        season: int | None,
        episode: int | None,
    ):
        """
        Fetch subtitle for a specific language.

        Args:
            item: MediaItem to fetch subtitle for
            language: ISO 639-3 language code
            video_path: Virtual VFS path of the video
            video_hash: OpenSubtitles hash of the video
            file_size: Size of the video file in bytes
            original_filename: Original filename of the video
            search_tags: Comma-separated tags (release group, format) for OpenSubtitles
            imdb_id: IMDB ID of the media
            season: Season number (for TV shows)
            episode: Episode number (for TV shows)
        """

        # Check if subtitle already exists
        existing_subtitle = self._get_existing_subtitle(item, language)

        if existing_subtitle:
            logger.debug(
                f"Subtitle for {language} already exists for {item.log_string}"
            )
            return

        # Search for subtitles across all providers
        all_results = list[SubtitleItem]()

        for provider in self.providers:
            try:
                results = provider.search_subtitles(
                    imdb_id=imdb_id or "",
                    video_hash=video_hash,
                    file_size=file_size,
                    filename=original_filename,
                    search_tags=search_tags,
                    season=season,
                    episode=episode,
                    language=language,
                )
                all_results.extend(results)
            except Exception as e:
                logger.error(
                    f"Provider {provider.name} failed to search subtitles: {e}"
                )

        if not all_results:
            logger.debug(f"No {language} subtitles found for {item.log_string}")
            return

        # Sort by score (highest first)
        all_results.sort(key=lambda x: x.score, reverse=True)

        # Try to download the best subtitle
        for subtitle_info in all_results[:3]:  # Try top 3 results
            try:
                provider_name = subtitle_info.provider
                provider = next(
                    (p for p in self.providers if p.name == provider_name), None
                )

                if not provider:
                    continue

                # Download subtitle content
                content = provider.download_subtitle(subtitle_info)

                if not content:
                    continue

                media_entry = item.media_entry

                assert media_entry

                # Get parent MediaEntry's original_filename
                parent_original_filename = media_entry.original_filename

                if not parent_original_filename:
                    logger.error(
                        f"MediaEntry for {item.log_string} has no original_filename, cannot create subtitle"
                    )
                    continue

                # Create SubtitleEntry and store in database
                subtitle_entry = SubtitleEntry.create_subtitle_entry(
                    language=language,
                    parent_original_filename=parent_original_filename,
                    content=content,
                    file_hash=video_hash,
                    video_file_size=media_entry.file_size,
                    opensubtitles_id=subtitle_info.id,
                )

                # Associate with media item
                subtitle_entry.media_item_id = item.id
                subtitle_entry.available_in_vfs = True

                # Save to database
                session = object_session(item)

                assert session

                session.add(subtitle_entry)

                # Flush to synchronize relationships before VFS sync
                # This ensures item.subtitles includes the new subtitle
                session.flush()

                logger.debug(
                    f"Downloaded and stored {language} subtitle for {item.log_string}"
                )

                from program.program import riven

                assert riven.services

                filesystem_service = riven.services.filesystem

                if filesystem_service and filesystem_service.riven_vfs:
                    filesystem_service.riven_vfs.sync(item)

                return

            except Exception as e:
                logger.error(
                    f"Failed to download subtitle from {subtitle_info.provider}: {e}"
                )
                if isinstance(e, (PendingRollbackError, OperationalError)):
                    _session = object_session(item)
                    if _session:
                        _session.rollback()

        logger.warning(
            f"Failed to download any {language} subtitle for {item.log_string}"
        )

    def _get_existing_subtitle(
        self, item: MediaItem, language: str
    ) -> SubtitleEntry | None:
        """
        Check if a subtitle already exists for the item and language.

        Args:
            item: MediaItem to check
            language: ISO 639-3 language code

        Returns:
            Existing SubtitleEntry or None
        """
        try:
            with db_session() as session:
                return (
                    session.query(SubtitleEntry)
                    .filter_by(media_item_id=item.id, language=language)
                    .first()
                )

        except Exception as e:
            logger.error(f"Failed to check for existing subtitle: {e}")
            return None

    def should_submit(self, item: MediaItem) -> bool:
        """
        Check if subtitles should be fetched for an item.

        Checks if:
        1. Item is a movie or episode
        2. Item has a filesystem entry
        3. At least one wanted language is missing (not embedded and not already downloaded)

        Args:
            item: MediaItem to check

        Returns:
            True if subtitles should be fetched
        """

        # Only fetch subtitles for movies and episodes
        if not isinstance(item, (Movie, Episode)):
            return False

        # Check if item has a filesystem entry
        if not item.filesystem_entry:
            return False

        # If subtitle service is not enabled, don't submit
        if not self.enabled:
            return False

        # Get embedded subtitle languages from media_metadata (ffprobe)
        embedded_languages = self._get_embedded_subtitle_languages(item)

        # Get already downloaded subtitle languages from database
        downloaded_languages = set[str]()

        try:
            with db_session() as session:
                existing_subtitles = (
                    session.query(SubtitleEntry).filter_by(media_item_id=item.id).all()
                )
                downloaded_languages = {sub.language for sub in existing_subtitles}
        except Exception as e:
            logger.warning(
                f"Failed to check existing subtitles for {item.log_string}: {e}"
            )

        # Combine embedded and downloaded languages
        available_languages = embedded_languages | downloaded_languages

        # Check if any wanted language is missing
        languages = self._parse_languages(language_codes=self.settings.languages)

        missing_languages = set(languages) - available_languages

        if not missing_languages:
            logger.debug(
                f"All wanted subtitle languages already available for {item.log_string}"
            )
            return False

        logger.debug(
            f"Missing subtitle languages for {item.log_string}: {missing_languages} "
            f"(embedded: {embedded_languages}, downloaded: {downloaded_languages})"
        )

        return True
