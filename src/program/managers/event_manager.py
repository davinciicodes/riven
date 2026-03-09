from dataclasses import dataclass
from enum import Enum
import json
import time
import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from queue import Empty
from threading import Lock
from typing import TYPE_CHECKING

import sqlalchemy.orm
from sqlalchemy.exc import OperationalError, PendingRollbackError
from sqlalchemy.orm.exc import StaleDataError
from loguru import logger
from pydantic import BaseModel

from program.db import db_functions
from program.db.db import db_session
from program.managers.sse_manager import sse_manager
from program.media.item import MediaItem
from program.types import Event, Service
from program.media.state import States

if TYPE_CHECKING:
    from program.program import Program


class EventUpdate(BaseModel):
    item_id: int
    emitted_by: str
    run_at: str


@dataclass
class ServiceExecutor:
    service_name: str
    executor: ThreadPoolExecutor


@dataclass(frozen=True)
class FutureWithEvent:
    future: Future[int | tuple[int, datetime] | None]
    event: Event | None
    cancellation_event: threading.Event


class EventType(Enum):
    Completed = 0
    PartiallyCompleted = 1
    Symlinked = 2
    Downloaded = 3
    Scraped = 4


class EventManager:
    """
    Manages the execution of services and the handling of events.
    """

<<<<<<< Updated upstream
    _MULTI_WORKER_SERVICES = {
        "Scraping": 2,
        "Downloader": 1,
        "Symlinker": 1,
        "PostProcessing": 1,
    }
=======
    _MULTI_WORKER_SERVICES = {"Scraping": 2, "Downloader": 1, "Symlinker": 1, "PostProcessing": 1}
>>>>>>> Stashed changes

    # Priority for the state-based queue ordering (lower = higher priority).
    # Items without a cached state get 999 (lowest) so they don't jump the queue.
    _STATE_PRIORITY: dict[States, int] = {
        States.Completed: 0,
        States.PartiallyCompleted: 1,
        States.Symlinked: 2,
        States.Downloaded: 3,
        States.Scraped: 4,
        States.Indexed: 5,
    }

    def __init__(self):
        self._executors = list[ServiceExecutor]()
        self._futures = list[FutureWithEvent]()
        self._queued_events = list[Event]()
        self._running_events = list[Event]()
        # Parallel sets for O(1) ID membership tests (kept in sync with the lists above).
        self._queued_item_ids: set[int] = set()
        self._running_item_ids: set[int] = set()
        self.mutex = Lock()
        # Condition variable (wraps self.mutex) used to wake the main loop immediately
        # when a new event is enqueued, replacing 100ms poll-sleep.
        self._work_available = threading.Condition(self.mutex)

    def _find_or_create_executor(self, service_cls: Service) -> ThreadPoolExecutor:
        """
        Finds or creates a ThreadPoolExecutor for the given service class.

        Args:
            service_cls (Service): The service class for which to find or create an executor.

        Returns:
            concurrent.futures.ThreadPoolExecutor: The executor for the service class.
        """

        service_name = service_cls.__class__.__name__

        for service_executor in self._executors:
            if service_executor.service_name == service_name:
                logger.log(
                    "TRACE",
                    f"_find_or_create_executor: Found existing executor for {service_name}",
                )
                logger.debug(f"Executor for {service_name} found.")

                return service_executor.executor

        workers = self._MULTI_WORKER_SERVICES.get(service_name, 1)
        logger.log(
            "TRACE",
            f"_find_or_create_executor: Creating new executor for {service_name} with {workers} workers",
        )

        _executor = ThreadPoolExecutor(
            thread_name_prefix=service_name,
            max_workers=workers,
        )

        self._executors.append(
            ServiceExecutor(service_name=service_name, executor=_executor)
        )

        logger.debug(f"Created executor for {service_name} with {workers} workers")

        return _executor

    def _process_future(self, future_with_event: FutureWithEvent, service: Service):
        """
        Processes the result of a future once it is completed.

        Args:
            future (concurrent.futures.Future): The future to process.
            service (type): The service class associated with the future.
        """

        logger.log(
            "TRACE",
            f"_process_future CALLED for service={service.__class__.__name__}, event={future_with_event.event.log_message if future_with_event.event else None}",
        )

        if future_with_event.future.cancelled():
            if future_with_event.event:
                logger.debug(
                    f"Future for {future_with_event.event.log_message} was cancelled."
                )
            else:
                logger.debug(f"Future for {future_with_event} was cancelled.")
            return  # Skip processing if the future was cancelled

        try:
            result = future_with_event.future.result()
            logger.log(
                "TRACE",
                f"_process_future RESULT: result={result} (type={type(result).__name__})",
            )

            if future_with_event in self._futures:
                self._futures.remove(future_with_event)

            sse_manager.publish_event(
                "event_update", json.dumps(self.get_event_updates())
            )

            if isinstance(result, tuple):
                item_id, timestamp = result
            else:
                item_id, timestamp = result, datetime.now()

            logger.log(
                "TRACE",
                f"_process_future PARSED: item_id={item_id}, timestamp={timestamp}",
            )

            if item_id:
                if future_with_event.event:
                    self.remove_event_from_running(future_with_event.event)

                    logger.debug(
                        f"Removed {future_with_event.event.log_message} from running events."
                    )

                if future_with_event.cancellation_event.is_set():
                    logger.debug(
                        f"Future with Item ID: {item_id} was cancelled; discarding results..."
                    )

                    return

                # Propagate overrides to the new event to maintain setting context across service transitions
                event_overrides = (
                    future_with_event.event.overrides
                    if future_with_event.event
                    else None
                )

                logger.log(
                    "TRACE",
                    f"_process_future QUEUING: Calling add_event for item_id={item_id}, emitted_by={service.__class__.__name__}, run_at={timestamp}",
                )
                self.add_event(
                    Event(
                        emitted_by=service,
                        item_id=item_id,
                        run_at=timestamp,
                        overrides=event_overrides,
                    )
                )
            else:
                logger.log(
                    "TRACE",
                    f"_process_future SKIP: item_id is None/False, not calling add_event",
                )
        except Exception as e:
            logger.error(f"Error in future for {future_with_event}: {e}")
            logger.exception(traceback.format_exc())

            if isinstance(e, (PendingRollbackError, OperationalError, StaleDataError)):
                item_id = future_with_event.event.item_id if future_with_event.event else None
                if item_id:
                    try:
                        with db_session() as session:
                            session.execute(
                                sqlalchemy.text('UPDATE "MediaItem" SET last_state = \'Indexed\', scraped_at = NULL WHERE id = :id'),
                                {"id": item_id}
                            )
                        logger.warning(f"DB connection dropped for item {item_id} — reset to Indexed for retry")
                    except Exception as reset_err:
                        logger.error(f"Failed to reset item {item_id} after connection drop: {reset_err}")
                if future_with_event.event:
                    self.remove_event_from_running(future_with_event.event)

        log_message = f"Service {service.__class__.__name__} executed"

        if future_with_event.event:
            log_message += f" with {future_with_event.event.log_message}"

        logger.debug(log_message)

    def add_event_to_queue(self, event: Event, log_message: bool = True):
        """
        Adds an event to the queue.

        Args:
            event (Event): The event to add to the queue.
        """

        with self.mutex:
            logger.log(
                "TRACE",
                f"add_event_to_queue: Attempting to queue event={event.log_message}, item_id={event.item_id}, emitted_by={event.emitted_by}",
            )

            if event.item_id and event.item_state is None:
                # State not pre-fetched (e.g. direct StateTransition call); fetch now.
                with db_session() as session:
                    try:
                        item = (
                            session.query(MediaItem)
                            .filter_by(id=event.item_id)
                            .options(
                                sqlalchemy.orm.load_only(
                                    MediaItem.id, MediaItem.last_state
                                )
                            )
                            .one_or_none()
                        )
                    except Exception as e:
                        logger.error(f"Error getting item from database: {e}")
                        return

                    if not item and not event.content_item:
                        logger.error(f"No item found from event: {event.log_message}")
                        return

                    if item:
                        if item.is_parent_blocked():
                            logger.debug(
                                f"Not queuing {item.log_string}: Item is {item.last_state}"
                            )
                            return

                        if item.last_state:
                            event.item_state = item.last_state
                            logger.log(
                                "TRACE",
                                f"add_event_to_queue: Cached item_state={item.last_state} for item_id={event.item_id}",
                            )
            elif event.item_id:
                logger.log(
                    "TRACE",
                    f"add_event_to_queue: Skipping DB query; item_state already cached as {event.item_state}",
                )

            queue_size_before = len(self._queued_events)
            self._queued_events.append(event)
            if event.item_id:
                self._queued_item_ids.add(event.item_id)
            logger.log(
                "TRACE",
                f"add_event_to_queue: Queue size changed from {queue_size_before} to {len(self._queued_events)}",
            )

            if log_message:
                logger.debug(f"Added {event.log_message} to the queue.")

            # Wake the main loop immediately instead of waiting for the next 100ms poll.
            self._work_available.notify()

    def remove_event_from_queue(self, event: Event):
        """
        Removes an event from the queue.

        Args:
            event (Event): The event to remove from the queue.
        """

        with self.mutex:
            queue_size_before = len(self._queued_events)
            self._queued_events.remove(event)
            if event.item_id:
                self._queued_item_ids.discard(event.item_id)
            logger.log(
                "TRACE",
                f"remove_event_from_queue: Queue size changed from {queue_size_before} to {len(self._queued_events)} after removing {event.log_message}",
            )
            logger.debug(f"Removed {event.log_message} from the queue.")

    def remove_event_from_running(self, event: Event):
        """
        Removes an event from the running events.

        Args:
            event (Event): The event to remove from the running events.
        """

        with self.mutex:
            if event in self._running_events:
                running_size_before = len(self._running_events)
                self._running_events.remove(event)
                if event.item_id:
                    self._running_item_ids.discard(event.item_id)
                logger.log(
                    "TRACE",
                    f"remove_event_from_running: Running events size changed from {running_size_before} to {len(self._running_events)} for {event.log_message}",
                )
                logger.debug(f"Removed {event.log_message} from running events.")

    def remove_id_from_queue(self, item_id: int):
        """
        Removes an item from the queue.

        Args:
            item (MediaItem): The event item to remove from the queue.
        """

        for event in self._queued_events:
            if event.item_id == item_id:
                self.remove_event_from_queue(event)

    def add_event_to_running(self, event: Event):
        """
        Adds an event to the running events.

        Args:
            event (Event): The event to add to the running events.
        """

        with self.mutex:
            running_size_before = len(self._running_events)
            self._running_events.append(event)
            if event.item_id:
                self._running_item_ids.add(event.item_id)
            logger.log(
                "TRACE",
                f"add_event_to_running: Running events size changed from {running_size_before} to {len(self._running_events)} for {event.log_message}",
            )
            logger.debug(f"Added {event.log_message} to running events.")

    def remove_id_from_running(self, item_id: int):
        """
        Removes an item from the running events.

        Args:
            item (MediaItem): The event item to remove from the running events.
        """

        for event in self._running_events:
            if event.item_id == item_id:
                self.remove_event_from_running(event)

    def remove_id_from_queues(self, item_id: int):
        """
        Removes an item from both the queue and the running events.

        Args:
            item_id: The event item to remove from both the queue and the running events.
        """

        self.remove_id_from_queue(item_id)
        self.remove_id_from_running(item_id)

    def submit_job(
        self,
        service: Service,
        program: "Program",
        event: Event | None = None,
    ) -> None:
        """
        Submits a job to be executed by the service.

        Args:
            service (type): The service class to execute.
            program (Program): The program containing the service.
            item (Event, optional): The event item to process. Defaults to None.
        """

        log_message = f"Submitting service {service.__class__.__name__} to be executed"

        # Content services dont provide an event.
        if event:
            log_message += f" with {event.log_message}"

        logger.debug(log_message)
        logger.log(
            "TRACE",
            f"submit_job: service={service.__class__.__name__}, event={event.log_message if event else None}, item_id={event.item_id if event else None}",
        )

        cancellation_event = threading.Event()

        executor = self._find_or_create_executor(service)
        logger.log(
            "TRACE", f"submit_job: Got executor for {service.__class__.__name__}"
        )

        assert program.services

        runner = program.services[service.get_key()]
        logger.log(
            "TRACE",
            f"submit_job: Got runner {runner.__class__.__name__} for key {service.get_key()}",
        )

        futures_count_before = len(self._futures)
        logger.log(
            "TRACE",
            f"submit_job: Submitting to executor (current futures count: {futures_count_before})",
        )

        future = executor.submit(
            db_functions.run_thread_with_db_item,
            runner.run,
            service,
            program,
            event,
            cancellation_event,
        )

        future_with_event = FutureWithEvent(
            future=future,
            event=event,
            cancellation_event=cancellation_event,
        )

        self._futures.append(future_with_event)
        logger.log(
            "TRACE",
            f"submit_job: Future submitted and stored (futures count now: {len(self._futures)})",
        )

        sse_manager.publish_event(
            "event_update",
            json.dumps(self.get_event_updates()),
        )

        future.add_done_callback(
            lambda f: self._process_future(future_with_event, service),
        )
        logger.log(
            "TRACE",
            f"submit_job: Done callback registered for {service.__class__.__name__}",
        )

    def cancel_job(self, item_id: int, suppress_logs: bool = False):
        """
        Cancels a job associated with the given item.

        Args:
            item_id (int): The event item whose job needs to be canceled.
            suppress_logs (bool): If True, suppresses debug logging for this operation.
        """

        with db_session() as session:
            item_id, related_ids = db_functions.get_item_ids(session, item_id)
            ids_to_cancel = set([item_id] + related_ids)

            future_map = dict[int, list[FutureWithEvent]]()

            for future_with_event in self._futures:
                if future_with_event.event and future_with_event.event.item_id:
                    future_item_id = future_with_event.event.item_id
                    future_map.setdefault(future_item_id, []).append(future_with_event)

            for fid in ids_to_cancel:
                if fid in future_map:
                    for future_with_event in future_map[fid]:
                        self.remove_id_from_queues(fid)

                        if (
                            not future_with_event.future.done()
                            and not future_with_event.future.cancelled()
                        ):
                            try:
                                future_with_event.cancellation_event.set()
                                future_with_event.future.cancel()

                                logger.debug(f"Canceled job for Item ID {fid}")
                            except Exception as e:
                                if not suppress_logs:
                                    logger.error(
                                        f"Error cancelling future for {fid}: {str(e)}"
                                    )

            for fid in ids_to_cancel:
                self.remove_id_from_queues(fid)

    def next(self, timeout: float = 1.0) -> Event:
        """
        Block until a ready event is available, then return it.

        Priority order (highest to lowest):
        0. Items in Completed state (closest to completion)
        1. Items in PartiallyCompleted state
        2. Items in Symlinked state
        3. Items in Downloaded state
        4. Items in Scraped state
        5. Items in Indexed state
        6. All other / unknown states (priority 999)

        Within each priority level, events are sorted by run_at timestamp.

        Performance: Uses _STATE_PRIORITY class constant and cached item_state from
        the Event to avoid per-call dict construction and database queries.

        Args:
            timeout: Maximum seconds to wait for a ready event. Defaults to 1.0.

        Raises:
            Empty: If no ready event becomes available within `timeout` seconds.

        Returns:
            Event: The next event in the queue.
        """

        deadline = time.monotonic() + timeout

        with self._work_available:
            while True:
                now = datetime.now()
                ready_events = [
                    event for event in self._queued_events if event.run_at <= now
                ]

                if ready_events:
                    ready_events.sort(
                        key=lambda e: (
                            self._STATE_PRIORITY.get(e.item_state, 999),
                            e.run_at,
                        )
                    )
                    event = ready_events[0]
                    logger.log(
                        "TRACE",
                        f"next: Selected event {event.log_message} with item_state={event.item_state}, run_at={event.run_at}",
                    )
                    queue_size_before = len(self._queued_events)
                    self._queued_events.remove(event)
                    if event.item_id:
                        self._queued_item_ids.discard(event.item_id)
                    logger.log(
                        "TRACE",
                        f"next: Removed selected event from queue (size: {queue_size_before} -> {len(self._queued_events)})",
                    )
                    return event

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Empty

                # If there are future-scheduled events, only wait until the earliest one.
                if self._queued_events:
                    earliest = min(e.run_at for e in self._queued_events)
<<<<<<< Updated upstream
                    wait_secs = max(
                        0.0, min(remaining, (earliest - now).total_seconds())
                    )
=======
                    wait_secs = max(0.0, min(remaining, (earliest - now).total_seconds()))
>>>>>>> Stashed changes
                else:
                    wait_secs = remaining

                self._work_available.wait(timeout=wait_secs)

    def _id_in_queue(self, _id: int) -> bool:
        """
        Checks if an item with the given ID is in the queue.

        Args:
            _id (int): The ID of the item to check.

        Returns:
            bool: True if the item is in the queue, False otherwise.
        """

        return _id in self._queued_item_ids

    def _id_in_running_events(self, _id: int) -> bool:
        """
        Checks if an item with the given ID is in the running events.

        Args:
            _id (int): The ID of the item to check.

        Returns:
            bool: True if the item is in the running events, False otherwise.
        """

        return _id in self._running_item_ids

    def add_event(self, event: Event) -> bool:
        """
        Adds an event to the queue if it is not already present in the queue or running events.

        - If the event has a DB-backed item_id, we keep your existing parent/child
        dedupe logic based on item_id + related ids.
        - If the event is content-only (no item_id), we now dedupe using *all* known ids
        (tmdb/tvdb/imdb) against both queued and running events with a single-pass check.

        Returns:
            True if queued; False if deduped away.
        """

        item_id = None
        related_ids = []

        # Check if the event's item is a show and its seasons or episodes are in the queue or running.
        # Also prefetch last_state here to avoid a second DB session in add_event_to_queue.
        with db_session() as session:
            if event.item_id:
                item_id, related_ids = db_functions.get_item_ids(session, event.item_id)
                if event.item_state is None:
                    row = (
                        session.query(MediaItem)
                        .filter_by(id=event.item_id)
                        .options(
<<<<<<< Updated upstream
                            sqlalchemy.orm.load_only(MediaItem.id, MediaItem.last_state)
=======
                            sqlalchemy.orm.load_only(
                                MediaItem.id, MediaItem.last_state
                            )
>>>>>>> Stashed changes
                        )
                        .one_or_none()
                    )
                    if row and row.last_state:
                        event.item_state = row.last_state

        if item_id:
            if self._id_in_queue(item_id):
                logger.debug(f"Item ID {item_id} is already in the queue, skipping.")
                return False

            if self._id_in_running_events(item_id):
                logger.debug(f"Item ID {item_id} is already running, skipping.")
                return False

            for related_id in related_ids:
                if self._id_in_queue(related_id) or self._id_in_running_events(
                    related_id
                ):
                    logger.debug(
                        f"Child of Item ID {item_id} is already in the queue or running, skipping."
                    )

                    return False
        else:
            # Content-only
            if (content_item := event.content_item) is None:
                logger.debug("Event has neither item_id nor content_item; skipping.")
                return False

            # Single-pass checks: queued and running
            if self.item_exists_in_queue(
                content_item,
                self._queued_events,
            ) or self.item_exists_in_queue(
                content_item,
                self._running_events,
            ):
                logger.debug(
                    f"Content Item with {content_item.log_string} is already queued or running, skipping."
                )

                return False

        self.add_event_to_queue(event)

        return True

    def add_item(
        self,
        item: MediaItem,
        service: str | None = None,
    ) -> bool:
        """
        Adds an item to the queue as an event.

        Args:
            item (MediaItem): The item to add to the queue as an event.
        """

        if not db_functions.item_exists_by_any_id(
            item.id,
            item.tvdb_id,
            item.tmdb_id,
            item.imdb_id,
        ):
            if self.add_event(
                Event(
                    service or "Manual",
                    content_item=item,
                )
            ):
                logger.debug(f"Added item with {item.log_string} to the queue.")
                return True

        return False

    def get_event_updates(self) -> dict[str, list[int]]:
        """
        Get the event updates for the SSE manager.

        Returns:
            dict[str, list[int]]: A dictionary with the event types as keys and a list of item IDs as values.
        """

        events = [future.event for future in self._futures if future.event]
        event_types = [
            "Scraping",
            "Downloader",
            "Symlinker",
            "Updater",
            "PostProcessing",
        ]

        updates = {event_type: list[int]() for event_type in event_types}

        for event in events:
            if isinstance(event.emitted_by, str):
                key = event.emitted_by
            else:
                key = event.emitted_by.__class__.__name__

            table = updates.get(key, None)

            if table is not None and event.item_id:
                table.append(event.item_id)

        return updates

    def item_exists_in_queue(self, item: MediaItem, queue: list[Event]) -> bool:
        """
        Check in a single pass whether any of the item's identifying ids (id, tmdb_id,
        tvdb_id, imdb_id) is already represented in the given event queue.

        This avoids building temporary sets (lower allocs) and returns early on first match.
        Worst-case O(n), typically faster in practice.

        Args:
            item: The media item to check. Only non-None ids are considered.
            queue: The event list to search.

        Returns:
            True if a match is found; otherwise False.
        """

        item_id = item.id
        tmdb_id = item.tmdb_id
        tvdb_id = item.tvdb_id
        imdb_id = item.imdb_id

        if not (item_id or tmdb_id or tvdb_id or imdb_id):
            return False

        for ev in queue:
            if item_id and ev.item_id == item_id:
                return True

            if (content_item := ev.content_item) is None:
                continue

            if tmdb_id and content_item.tmdb_id == tmdb_id:
                return True

            if tvdb_id and content_item.tvdb_id == tvdb_id:
                return True

            if imdb_id and content_item.imdb_id == imdb_id:
                return True

        return False
