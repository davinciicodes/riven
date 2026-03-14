from typing import Any, Literal
from kink import di
from loguru import logger

from program.media import MediaItem, States
from program.types import ProcessedEvent, Service
from program.media.item import Season, Show


def process_event(
    emitted_by: Service | Literal["StateTransition", "RetryLibrary"] | str,
    existing_item: MediaItem | None = None,
    content_item: MediaItem | None = None,
    overrides: dict[str, Any] | None = None,
) -> ProcessedEvent:
    """Process an event and return the updated item, next service and items to submit."""

    from program.program import Program

    services = di[Program].services

    assert services

    next_service: Service | None = None
    no_further_processing = ProcessedEvent(
        service=None,
        related_media_items=[],
    )
    items_to_submit = list[MediaItem]()

    if existing_item and existing_item.last_state in [States.Paused, States.Failed]:
        return no_further_processing

    if content_item or (existing_item and existing_item.last_state == States.Requested):
        log_string = None

        if existing_item:
            log_string = existing_item.log_string
        elif content_item:
            log_string = content_item.log_string

        logger.debug(f"Submitting {log_string} to IndexerService")

        related_media_items = list[MediaItem]()

        if content_item:
            related_media_items.append(content_item)
        elif existing_item:
            related_media_items.append(existing_item)

        return ProcessedEvent(
            service=services.indexer,
            related_media_items=related_media_items,
            overrides=overrides,
        )

    elif existing_item and existing_item.last_state in [
        States.PartiallyCompleted,
        States.Ongoing,
    ]:
        if isinstance(existing_item, Show):
            incomplete_seasons = [
                s
                for s in existing_item.seasons
                if s.last_state not in [States.Completed, States.Unreleased]
            ]

            for season in incomplete_seasons:
                processed_event = process_event(emitted_by, season, None, overrides)

                if processed_event.related_media_items:
                    items_to_submit += processed_event.related_media_items

            # All downloadable seasons are done but show stays Ongoing (still airing).
            # Route to PostProcessing so completed episodes can get subtitles.
            if not items_to_submit and emitted_by != services.post_processing:
                next_service = services.post_processing
                items_to_submit = [existing_item]

        elif isinstance(existing_item, Season):
            incomplete_episodes = [
                e for e in existing_item.episodes if e.last_state != States.Completed
            ]

            for episode in incomplete_episodes:
                processed_event = process_event(emitted_by, episode, None, overrides)

                if processed_event.related_media_items:
                    items_to_submit += processed_event.related_media_items

            # All episodes done but season stays PartiallyCompleted/Ongoing.
            if not items_to_submit and emitted_by != services.post_processing:
                next_service = services.post_processing
                items_to_submit = [existing_item]

    elif existing_item and existing_item.last_state == States.Indexed:
        next_service = services.scraping

        if emitted_by != services.scraping and (
            overrides is not None or services.scraping.should_submit(existing_item)
        ):
            items_to_submit = [existing_item]
        elif isinstance(existing_item, Show):
            items_to_submit = [
                s
                for s in existing_item.seasons
                if s.last_state
                in [States.Indexed, States.PartiallyCompleted, States.Unknown]
                and (overrides is not None or services.scraping.should_submit(s))
            ]
        elif isinstance(existing_item, Season):
            items_to_submit = [
                e
                for e in existing_item.episodes
                if e.last_state in [States.Indexed, States.Unknown]
                and (overrides is not None or services.scraping.should_submit(e))
            ]

    elif existing_item and existing_item.last_state == States.Unknown:
        # Treat Unknown state like Indexed - submit for scraping
        next_service = services.scraping

        if emitted_by != services.scraping and (
            overrides is not None or services.scraping.should_submit(existing_item)
        ):
            items_to_submit = [existing_item]
        elif isinstance(existing_item, Show):
            # Check if all seasons are already scraped (retry scenario)
            scraped_seasons = [
                s for s in existing_item.seasons if s.last_state == States.Scraped
            ]

            if scraped_seasons:
                # All/some seasons already scraped, delegate to each season's state logic
                for season in scraped_seasons:
                    # Recursively process scraped seasons
                    processed = process_event(emitted_by, season, None, overrides)
                    if processed.related_media_items:
                        items_to_submit += processed.related_media_items
                        if processed.service:
                            next_service = processed.service
            else:
                # Normal Unknown case - look for unscraped seasons
                items_to_submit = [
                    s
                    for s in existing_item.seasons
                    if s.last_state
                    in [States.Indexed, States.PartiallyCompleted, States.Unknown]
                    and (overrides is not None or services.scraping.should_submit(s))
                ]
        elif isinstance(existing_item, Season):
            # Check if all episodes are already scraped (retry scenario)
            scraped_episodes = [
                e for e in existing_item.episodes if e.last_state == States.Scraped
            ]

            if scraped_episodes:
                # Episodes already scraped, submit them to downloader
                next_service = services.downloader
                items_to_submit = scraped_episodes
            else:
                # Normal Unknown case - look for unscraped episodes
                items_to_submit = [
                    e
                    for e in existing_item.episodes
                    if e.last_state in [States.Indexed, States.Unknown]
                    and (overrides is not None or services.scraping.should_submit(e))
                ]

    elif existing_item and existing_item.last_state == States.Scraped:
        # For Shows, prioritize scraping seasons over downloading the show itself
        # Shows typically transition to PartiallyCompleted/Ongoing, not Downloaded
        if isinstance(existing_item, Show):
            unscraped_seasons = [
                s
                for s in existing_item.seasons
                if s.last_state in [States.Indexed, States.Unknown]
                and (overrides is not None or services.scraping.should_submit(s))
            ]

            if unscraped_seasons:
                # Submit seasons for scraping - this is the normal path for shows
                next_service = services.scraping
                items_to_submit = unscraped_seasons
            else:
                # All seasons already handled - try downloading show-level pack
                # (rare case: complete series pack with all seasons already done)
                next_service = services.downloader
                items_to_submit = [existing_item]
        elif isinstance(existing_item, Season):
            # For Seasons, prioritize scraping episodes over downloading the season
            unscraped_episodes = [
                e
                for e in existing_item.episodes
                if e.last_state in [States.Indexed, States.Unknown]
                and (overrides is not None or services.scraping.should_submit(e))
            ]

            if unscraped_episodes:
                # Submit episodes for scraping - normal path for seasons with episodes
                next_service = services.scraping
                items_to_submit = unscraped_episodes
            else:
                # All episodes handled - download season pack
                next_service = services.downloader
                items_to_submit = [existing_item]
        else:
            # Movies, Episodes go straight to downloader
            next_service = services.downloader
            items_to_submit = [existing_item]

    elif existing_item and existing_item.last_state == States.Downloaded:
        next_service = services.filesystem
        items_to_submit = [existing_item]

    elif existing_item and existing_item.last_state == States.Symlinked:
        next_service = services.updater
        items_to_submit = [existing_item]

    elif existing_item and existing_item.last_state == States.Completed:
        # Avoid multiple post-processing runs
        if emitted_by != services.post_processing:
            next_service = services.post_processing
            items_to_submit = [existing_item]
        else:
            return no_further_processing

    if items_to_submit:
        service_name = (
            next_service.__class__.__name__ if next_service else "StateTransition"
        )

        logger.debug(
            f"State transition complete: {len(items_to_submit)} items queued for {service_name}"
        )

    return ProcessedEvent(
        service=next_service,
        related_media_items=items_to_submit,
        overrides=overrides,
    )
