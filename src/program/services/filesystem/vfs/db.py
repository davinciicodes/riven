from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

from kink import di
from loguru import logger

from pydantic import BaseModel
from sqlalchemy.orm import Session
from program.db.db import db_session
from program.media.media_entry import MediaEntry
from program.services.streaming.exceptions import (
    DebridServiceLinkUnavailable,
)
from program.media.item import MediaItem
from program.types import Event
from routers.secure.items import apply_item_mutation
from program.utils.cdn_health import get_provider_score, sort_services_by_health
from program.utils.debrid_cdn_url import DebridCDNUrl

if TYPE_CHECKING:
    from program.services.downloaders import Downloader


class VFSEntry(TypedDict):
    virtual_path: str
    name: str
    size: int
    is_directory: bool
    entry_type: str | None
    created: str | None
    modified: str | None


class GetEntryByOriginalFilenameResult(BaseModel):
    original_filename: str
    download_url: str | None
    unrestricted_url: str | None
    provider: str | None
    provider_download_id: str | None
    size: int | None
    created: str | None
    modified: str | None
    entry_type: Literal["media", "subtitle"]

    @property
    def url(self) -> str | None:
        """The URL to use for this request."""

        return self.unrestricted_url or self.download_url


class VFSDatabase:
    def __init__(self, downloader: "Downloader | None" = None) -> None:
        """
        Initialize VFS Database.

        Args:
            downloader: Downloader instance with initialized services for URL resolution
        """

        self.downloader = downloader

    # --- Queries ---
    def get_subtitle_content(
        self,
        parent_original_filename: str,
        language: str,
    ) -> bytes | None:
        """
        Get the subtitle content for a SubtitleEntry.

        In the new architecture, subtitles are looked up by their parent video's
        original_filename and language code, not by path.

        Parameters:
            parent_original_filename (str): Original filename of the parent MediaEntry (video file).
            language (str): ISO 639-3 language code (e.g., 'eng').

        Returns:
            bytes: Subtitle content encoded as UTF-8, or None if not found or not a subtitle.
        """

        with db_session() as session:
            from program.media.subtitle_entry import SubtitleEntry

            # Query specifically for SubtitleEntry by parent and language
            subtitle = (
                session.query(SubtitleEntry)
                .filter_by(
                    parent_original_filename=parent_original_filename, language=language
                )
                .first()
            )

            if subtitle and subtitle.content:
                return subtitle.content.encode("utf-8")

            return None

    def refresh_unrestricted_url(
        self,
        entry: MediaEntry,
        session: Session,
    ) -> str | None:
        """
        Refresh the unrestricted URL for a MediaEntry using the downloader services.

        Args:
            entry: MediaEntry to refresh
        """

        if not self.downloader:
            logger.warning("No downloader available to refresh unrestricted URL")

            return None

        # If CDN health data shows the current provider is known-dead, skip the
        # re-unrestrict attempt (it would return another dead CDN URL) and go
        # straight to the alternate-provider fallback.
        if entry.provider and get_provider_score(entry.provider) == 0.0:
            logger.debug(
                f"CDN health: {entry.provider} is known dead, "
                f"skipping re-unrestrict for {entry.original_filename}"
            )
            return self._try_alternate_provider(entry, session)

        from program.program import Program

        # Find service by matching the key attribute (services dict uses class as key)
        service = next(
            (
                svc
                for svc in self.downloader.services.values()
                if svc.key == entry.provider
            ),
            None,
        )

        if service and entry.download_url:
            try:
                new_unrestricted = service.unrestrict_link(entry.download_url)

                if new_unrestricted:
                    entry.unrestricted_url = new_unrestricted.download

                    with session.no_autoflush:
                        cdn_url = DebridCDNUrl(entry)
                        cdn_validate_ok = cdn_url.validate(attempt_refresh=False)

                    if cdn_validate_ok:
                        session.merge(entry)
                        session.commit()

                        logger.debug(
                            f"Refreshed unrestricted URL for {entry.original_filename}"
                        )

                        return entry.unrestricted_url
                    # Primary provider gave a URL but its CDN is unreachable — fall through to alternate providers
            except DebridServiceLinkUnavailable as e:
                logger.warning(
                    f"Failed to unrestrict URL for {entry.original_filename}: {e}"
                )

                # If un-restricting fails, reset the MediaItem to trigger a new download
                if entry.media_item:
                    item_id = entry.media_item.id

                    def mutation(i: MediaItem, s: Session):
                        i.blacklist_active_stream()
                        i.reset()

                    apply_item_mutation(
                        program=di[Program],
                        item=entry.media_item,
                        mutation_fn=mutation,
                        session=session,
                    )

                    session.commit()

                    di[Program].em.add_event(
                        Event(
                            "VFS",
                            item_id,
                        )
                    )

                    return None
                raise
            except Exception as e:
                logger.warning(
                    f"Unexpected error when unrestricting URL for {entry.original_filename}: {e}"
                )
                # Reset session so _try_alternate_provider can run with a clean state.
                # The exception may have left the session in a rolled-back state, which
                # would cause any subsequent lazy-load (e.g. entry.media_item) to fail.
                try:
                    session.rollback()
                except Exception:
                    pass

        return self._try_alternate_provider(entry, session)

    def _try_alternate_provider(self, entry: MediaEntry, session: Session) -> str | None:
        """
        Try to get a working CDN URL from an alternate debrid provider.

        Called when the primary provider's CDN is unreachable. Iterates over other
        initialized services, checks instant availability for the same infohash,
        and switches the entry to the first provider whose CDN validates successfully.
        """

        if not self.downloader:
            return None

        media_item = entry.media_item
        if not media_item or not media_item.active_stream:
            return None

        infohash = media_item.active_stream.infohash
        item_type = media_item.type
        original_provider = entry.provider

        for service in sort_services_by_health(self.downloader.initialized_services):
            if service.key == original_provider:
                continue

            try:
                logger.info(
                    f"CDN fallback: trying {service.key} for {entry.original_filename} (infohash={infohash})"
                )

                container = service.get_instant_availability(infohash, item_type)

                if not container or not container.files:
                    logger.debug(
                        f"CDN fallback: {service.key} does not have {infohash} cached"
                    )
                    continue

                matching_file = next(
                    (f for f in container.files if f.filename == entry.original_filename),
                    None,
                )

                if not matching_file or not matching_file.download_url:
                    logger.debug(
                        f"CDN fallback: no matching file '{entry.original_filename}' on {service.key}"
                    )
                    continue

                unrestricted = service.unrestrict_link(matching_file.download_url)
                if not unrestricted:
                    continue

                # Temporarily mutate entry to test the new CDN URL
                old_provider = entry.provider
                old_download_url = entry.download_url
                old_unrestricted_url = entry.unrestricted_url
                old_provider_download_id = entry.provider_download_id

                entry.provider = service.key
                entry.download_url = matching_file.download_url
                entry.unrestricted_url = unrestricted.download
                if container.torrent_id:
                    entry.provider_download_id = str(container.torrent_id)

                cdn_ok = DebridCDNUrl(entry).validate(attempt_refresh=False)

                if not cdn_ok:
                    # Restore original values and try next service
                    entry.provider = old_provider
                    entry.download_url = old_download_url
                    entry.unrestricted_url = old_unrestricted_url
                    entry.provider_download_id = old_provider_download_id
                    logger.debug(
                        f"CDN fallback: {service.key} CDN URL also unreachable for {entry.original_filename}"
                    )
                    continue

                session.merge(entry)
                session.commit()

                logger.info(
                    f"CDN fallback: switched '{entry.original_filename}' "
                    f"from {original_provider} to {service.key}"
                )
                return entry.unrestricted_url

            except Exception as e:
                logger.warning(
                    f"CDN fallback: {service.key} failed for {entry.original_filename}: {e}"
                )
                continue

        logger.warning(
            f"CDN fallback: no alternate provider could serve {entry.original_filename}"
        )
        return None

    def reset_item_for_link_failure(self, original_filename: str) -> None:
        """Reset a MediaItem for re-scraping after repeated stream link failures."""

        try:
            from program.program import Program

            with db_session() as session:
                entry = (
                    session.query(MediaEntry)
                    .filter(MediaEntry.original_filename == original_filename)
                    .first()
                )

                if not entry or not entry.media_item:
                    logger.warning(
                        f"No item found to reset for repeated link failures on {original_filename}"
                    )
                    return

                item_id = entry.media_item.id

                def mutation(i: MediaItem, s: Session):
                    i.blacklist_active_stream()
                    i.reset()

                apply_item_mutation(
                    program=di[Program],
                    item=entry.media_item,
                    mutation_fn=mutation,
                    session=session,
                )

                session.commit()

                di[Program].em.add_event(Event("VFS", item_id))

                logger.info(
                    f"Re-queued item {item_id} for re-scrape after repeated link failures on {original_filename}"
                )
        except Exception as e:
            logger.error(
                f"Failed to reset item for {original_filename} after link failures: {e}"
            )

    def get_entry_by_original_filename(
        self,
        original_filename: str,
        force_resolve: bool = False,
    ) -> GetEntryByOriginalFilenameResult | None:
        """
        Get entry metadata and download URL by original filename.

        This is the NEW API that replaces path-based lookups.

        Args:
            original_filename: Original filename from debrid provider
            force_resolve: If True, force refresh of unrestricted URL from provider

        Returns:
            Dictionary with entry metadata and URLs, or None if not found
        """

        try:
            with db_session() as session:
                entry = (
                    session.query(MediaEntry)
                    .filter(MediaEntry.original_filename == original_filename)
                    .first()
                )

                if not entry:
                    return None

                # Get download URL (with optional unrestricting)
                download_url = entry.download_url
                unrestricted_url = entry.unrestricted_url

                # If force_resolve or no unrestricted URL, try to unrestrict
                if (force_resolve or not unrestricted_url) and (
                    self.downloader and entry.provider
                ):
                    unrestricted_url = self.refresh_unrestricted_url(
                        entry,
                        session=session,
                    )

                return GetEntryByOriginalFilenameResult(
                    original_filename=entry.original_filename,
                    download_url=download_url,
                    unrestricted_url=unrestricted_url,
                    provider=entry.provider,
                    provider_download_id=entry.provider_download_id,
                    size=entry.file_size,
                    created=(entry.created_at.isoformat()),
                    modified=(entry.updated_at.isoformat()),
                    entry_type="media",
                )
        except DebridServiceLinkUnavailable:
            raise
        except Exception as e:
            logger.error(
                f"Error getting entry by original_filename {original_filename}: {e}"
            )
            return None
