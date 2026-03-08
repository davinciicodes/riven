from kink import di
from pydantic import BaseModel, ConfigDict
from fastapi import APIRouter, Request
from loguru import logger
from sqlalchemy import or_, select
from sqlalchemy.orm import selectinload

from program.db.db import db_session
from program.media.item import Episode, MediaItem, Season, Show
from program.program import Program
from program.services.content.overseerr import Overseerr
from program.types import Event

from ..models.overseerr import OverseerrWebhook

router = APIRouter(
    prefix="/webhook",
    responses={404: {"description": "Not found"}},
)


class OverseerrWebhookResponse(BaseModel):
    success: bool
    message: str | None = None


@router.post(
    "/overseerr",
    response_model=OverseerrWebhookResponse,
)
async def overseerr(request: Request) -> OverseerrWebhookResponse:
    """Webhook for Overseerr"""

    try:
        response = await request.json()

        if response.get("subject") == "Test Notification":
            logger.log(
                "API", "Received test notification, Overseerr configured properly"
            )

            return OverseerrWebhookResponse(
                success=True,
            )

        req = OverseerrWebhook.model_validate(response)

        if services := di[Program].services:
            overseerr = services.overseerr
        else:
            logger.error("Overseerr not initialized yet")
            return OverseerrWebhookResponse(
                success=False,
                message="Overseerr not initialized",
            )

        if not overseerr.initialized:
            logger.error("Overseerr not initialized")

            return OverseerrWebhookResponse(
                success=False,
                message="Overseerr not initialized",
            )

        item_type = req.media.media_type

        new_item = None

        if item_type == "movie":
            new_item = MediaItem(
                {
                    "tmdb_id": req.media.tmdbId,
                    "requested_by": "overseerr",
                    "overseerr_id": req.request.request_id if req.request else None,
                }
            )
        elif item_type == "tv":
            new_item = MediaItem(
                {
                    "tvdb_id": req.media.tvdbId,
                    "requested_by": "overseerr",
                    "overseerr_id": req.request.request_id if req.request else None,
                }
            )

        if not new_item:
            logger.error(
                f"Failed to create new item: TMDB ID {req.media.tmdbId}, TVDB ID {req.media.tvdbId}"
            )

            return OverseerrWebhookResponse(
                success=False,
                message="Failed to create new item",
            )

        di[Program].em.add_item(
            new_item,
            service=Overseerr.__class__.__name__,
        )

        return OverseerrWebhookResponse(success=True)
    except Exception as e:
        logger.error(f"Failed to process request: {e}")

        return OverseerrWebhookResponse(success=False)


# ---------------------------------------------------------------------------
# Emby webhook
# ---------------------------------------------------------------------------

# 1 tick = 100 ns  →  10_000_000 ticks = 1 second
_TICKS_PER_SECOND = 10_000_000
# Positions below this (5 min) with no completion flag are treated as failures
_FAILURE_POSITION_THRESHOLD = 5 * 60 * _TICKS_PER_SECOND
# Also flag if progress is below 5% of total runtime (for short content)
_FAILURE_PROGRESS_FRACTION = 0.05


class _EmbyProviderIds(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    Tmdb: str | None = None
    Imdb: str | None = None
    Tvdb: str | None = None


class _EmbyPlayState(BaseModel):
    model_config = ConfigDict(extra="allow")

    PositionTicks: int | None = None


class _EmbySession(BaseModel):
    model_config = ConfigDict(extra="allow")

    PlayState: _EmbyPlayState | None = None


class _EmbyItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    Type: str | None = None
    ProviderIds: _EmbyProviderIds | None = None
    # For episodes the show-level IDs live here
    SeriesProviderIds: _EmbyProviderIds | None = None
    RunTimeTicks: int | None = None
    SeasonNumber: int | None = None
    IndexNumber: int | None = None  # episode number within the season


class _EmbyWebhookPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    # Plugin implementations differ on this field name
    Event: str | None = None
    NotificationType: str | None = None
    Item: _EmbyItem | None = None
    Session: _EmbySession | None = None
    PlayedToCompletion: bool | None = None
    # Some plugins flatten position here instead of Session.PlayState
    PlaybackPositionTicks: int | None = None


class EmbyWebhookResponse(BaseModel):
    success: bool
    message: str | None = None


@router.post("/emby", response_model=EmbyWebhookResponse)
async def emby(request: Request) -> EmbyWebhookResponse:
    """
    Webhook for Emby playback events.

    Detects failed playback (PlaybackStop very early in the file without
    completion) and re-queues the affected item for scraping automatically.

    Configure the Emby Webhook plugin to POST to:
        http://riven:8080/api/v1/webhook/emby?apikey=<your-api-key>
    with events: PlaybackStop (media.stop).
    """

    try:
        body = await request.json()
        payload = _EmbyWebhookPayload.model_validate(body)

        event = (payload.Event or payload.NotificationType or "").lower()

        # Test ping — acknowledge and do nothing
        if event in ("system.webhooktest", "test"):
            logger.log("API", "Received Emby test notification")
            return EmbyWebhookResponse(success=True, message="Test acknowledged")

        # Only act on stop events
        if event not in ("media.stop", "playbackstop"):
            return EmbyWebhookResponse(success=True, message=f"Ignored event: {event}")

        if not payload.Item:
            return EmbyWebhookResponse(success=False, message="Missing Item in payload")

        # Completed normally → nothing to do
        if payload.PlayedToCompletion:
            return EmbyWebhookResponse(success=True, message="Played to completion")

        # Resolve playback position from payload (two common locations)
        position = (
            payload.PlaybackPositionTicks
            or (
                payload.Session
                and payload.Session.PlayState
                and payload.Session.PlayState.PositionTicks
            )
            or 0
        )
        runtime = payload.Item.RunTimeTicks or 0

        early_stop = position < _FAILURE_POSITION_THRESHOLD
        low_progress = runtime > 0 and position < runtime * _FAILURE_PROGRESS_FRACTION

        if not (early_stop or low_progress):
            return EmbyWebhookResponse(success=True, message="Not an early stop")

        item_type = (payload.Item.Type or "").lower()
        ids = payload.Item.ProviderIds or _EmbyProviderIds()
        series_ids = payload.Item.SeriesProviderIds or _EmbyProviderIds()

        target_id: int | None = None

        with db_session() as session:
            if item_type == "movie":
                conditions = []
                if ids.Tmdb:
                    conditions.append(MediaItem.tmdb_id == ids.Tmdb)
                if ids.Imdb:
                    conditions.append(MediaItem.imdb_id == ids.Imdb)
                if ids.Tvdb:
                    conditions.append(MediaItem.tvdb_id == ids.Tvdb)

                if conditions:
                    row = session.execute(
                        select(MediaItem)
                        .where(MediaItem.type == "movie")
                        .where(or_(*conditions))
                    ).scalar_one_or_none()

                    if row:
                        target_id = row.id

            elif item_type == "episode":
                # Find the show by its TVDB/TMDB/IMDB ID (series-level IDs)
                show_conditions = []
                for id_val, col in [
                    (series_ids.Tvdb or ids.Tvdb, MediaItem.tvdb_id),
                    (series_ids.Tmdb or ids.Tmdb, MediaItem.tmdb_id),
                    (series_ids.Imdb or ids.Imdb, MediaItem.imdb_id),
                ]:
                    if id_val:
                        show_conditions.append(col == id_val)

                if show_conditions:
                    show = session.execute(
                        select(Show)
                        .options(
                            selectinload(Show.seasons).selectinload(Season.episodes)
                        )
                        .where(or_(*show_conditions))
                    ).scalar_one_or_none()

                    season_num = payload.Item.SeasonNumber
                    ep_num = payload.Item.IndexNumber

                    if show and season_num is not None and ep_num is not None:
                        for season in show.seasons:
                            if season.number == season_num:
                                for ep in season.episodes:
                                    if ep.number == ep_num:
                                        target_id = ep.id
                                        break

            if target_id is None:
                logger.warning(
                    f"Emby webhook: could not find Riven item for {item_type} "
                    f"ids={ids} series_ids={series_ids}"
                )
                return EmbyWebhookResponse(
                    success=False, message="Item not found in Riven"
                )

            item = session.get(MediaItem, target_id)

            if not item:
                return EmbyWebhookResponse(
                    success=False, message="Item not found in Riven"
                )

            item.scraped_at = None
            item.scraped_times = 1
            session.commit()

        di[Program].em.add_event(Event("RetryItem", target_id))

        logger.log(
            "API",
            f"Emby webhook: re-queued item {target_id} ({item_type}) after early stop "
            f"at {position // _TICKS_PER_SECOND}s / {runtime // _TICKS_PER_SECOND}s",
        )

        return EmbyWebhookResponse(success=True, message=f"Re-queued item {target_id}")

    except Exception as e:
        logger.error(f"Emby webhook error: {e}")
        return EmbyWebhookResponse(success=False, message=str(e))
