"""
CDN health monitoring for debrid provider selection.

Runs as an APScheduler job every 5 minutes: queries the DB for recent
unrestricted URLs, deduplicates by CDN hostname, and measures latency +
throughput via small range requests. Results are kept in memory and used to
sort provider candidates in _try_alternate_provider() — best throughput first,
known-dead providers last.

Thread safety: a single threading.Lock guards _host_stats. APScheduler writes
from its thread; VFS streaming reads from request threads.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import httpx
from loguru import logger

# --- Config ------------------------------------------------------------------

_TEST_BYTES = 256 * 1024  # 256 KB range request per node
_TEST_TIMEOUT = 10  # seconds per host test
# No time window — test all unique CDN hostnames from the DB.
# Expired URLs return 404 (handled gracefully), ConnectError marks nodes dead.

# --- State -------------------------------------------------------------------

_lock = threading.Lock()
_host_stats: dict[str, _HostStat] = {}  # hostname → stats


@dataclass
class _HostStat:
    provider: str
    throughput_mbps: float = 0.0
    latency_ms: float = 0.0
    is_reachable: bool = True
    last_tested: float = field(default_factory=time.monotonic)


# --- Public API --------------------------------------------------------------


def get_provider_score(provider_key: str) -> float:
    """
    Return the average throughput (Mbps) for a provider's CDN nodes.

    Returns:
        > 0.0  — measured and reachable (higher = better)
        0.0    — all known nodes are unreachable (known dead)
        -1.0   — no data yet (provider not yet measured)
    """
    with _lock:
        stats = [s for s in _host_stats.values() if s.provider == provider_key]

    if not stats:
        return -1.0

    reachable = [s for s in stats if s.is_reachable]

    if not reachable:
        return 0.0

    return sum(s.throughput_mbps for s in reachable) / len(reachable)


def sort_services_by_health(services: list) -> list:
    """
    Sort debrid services by CDN health, best first.

    Priority order:
      1. Unknown providers (no data yet), in their original insertion order
         — tried first so we gather throughput data and don't permanently
           favour a measured-but-slow provider over an unmeasured one
      2. Known-working providers, sorted by average throughput descending
      3. Known-dead providers (all nodes unreachable), last

    Args:
        services: List of downloader service instances with a .key attribute.

    Returns:
        New list sorted by CDN health.
    """

    def sort_key(idx_service: tuple[int, object]) -> tuple[int, int, float]:
        idx, service = idx_service
        score = get_provider_score(service.key)  # type: ignore[attr-defined]

        if score < 0.0:
            return (0, idx, 0.0)   # unknown: first, preserve original order
        elif score > 0.0:
            return (1, 0, -score)  # known-working: sort by throughput desc
        else:
            return (2, idx, 0.0)   # dead: last

    return [s for _, s in sorted(enumerate(services), key=sort_key)]


def record_connect_error(url: str, provider: str) -> None:
    """
    Mark a CDN hostname as unreachable immediately on ConnectError.

    Called from DebridCDNUrl.validate() so the health data is updated
    reactively without waiting for the next scheduled check.
    """
    hostname = urlparse(url).hostname

    if not hostname:
        return

    with _lock:
        existing = _host_stats.get(hostname)

        if existing:
            existing.is_reachable = False
            existing.throughput_mbps = 0.0
            existing.last_tested = time.monotonic()
        else:
            _host_stats[hostname] = _HostStat(
                provider=provider,
                throughput_mbps=0.0,
                latency_ms=0.0,
                is_reachable=False,
            )

    logger.debug(f"CDN health: marked {hostname} ({provider}) as unreachable")


# --- APScheduler job ---------------------------------------------------------


def check_cdn_health() -> None:
    """
    APScheduler job: probe CDN nodes from recent MediaEntry unrestricted URLs.

    Queries the DB for entries updated within the last hour (URLs are likely
    still valid), deduplicates by hostname, and fires a 256 KB range request
    to each unique CDN node to measure latency and throughput.
    """
    try:
        from program.db.db import db_session
        from program.media.media_entry import MediaEntry

        with db_session() as session:
            rows = (
                session.query(MediaEntry.unrestricted_url, MediaEntry.provider)
                .filter(
                    MediaEntry.unrestricted_url.isnot(None),
                    MediaEntry.provider.isnot(None),
                )
                .all()
            )

        # Deduplicate: one URL per hostname (first seen wins)
        hosts_to_test: dict[str, tuple[str, str]] = {}
        for url, provider in rows:
            hostname = urlparse(url).hostname
            if hostname and hostname not in hosts_to_test:
                hosts_to_test[hostname] = (url, provider)

        if not hosts_to_test:
            logger.trace("CDN health check: no recent CDN URLs found in DB")
            return

        logger.debug(f"CDN health check: testing {len(hosts_to_test)} CDN node(s)")

        for hostname, (url, provider) in hosts_to_test.items():
            _test_host(hostname, url, provider)

    except Exception as e:
        logger.warning(f"CDN health check failed: {e}")


# --- Internal ----------------------------------------------------------------


def _test_host(hostname: str, url: str, provider: str) -> None:
    """Download 256 KB from a CDN URL and record latency + throughput."""
    try:
        headers = {"Range": f"bytes=0-{_TEST_BYTES - 1}"}
        start = time.monotonic()

        with httpx.Client(timeout=_TEST_TIMEOUT) as client:
            with client.stream("GET", url, headers=headers) as resp:
                if resp.status_code not in (200, 206):
                    # Non-2xx is likely an expired URL, not a dead CDN host.
                    # Don't mark as unreachable — just skip this node.
                    logger.debug(
                        f"CDN health: {hostname} ({provider}) HTTP {resp.status_code} "
                        f"(URL likely expired, skipping)"
                    )
                    return

                latency_ms = (time.monotonic() - start) * 1000

                download_start = time.monotonic()
                total = sum(len(chunk) for chunk in resp.iter_bytes(chunk_size=65536))
                elapsed = time.monotonic() - download_start

        throughput_mbps = (total * 8 / elapsed / 1_000_000) if elapsed > 0 else 0.0

        with _lock:
            _host_stats[hostname] = _HostStat(
                provider=provider,
                throughput_mbps=throughput_mbps,
                latency_ms=latency_ms,
                is_reachable=True,
            )

        logger.debug(
            f"CDN health: {hostname} ({provider}) — "
            f"{latency_ms:.0f} ms latency, {throughput_mbps:.1f} Mbps"
        )

    except httpx.ConnectError:
        _mark_unreachable(hostname, provider)
        logger.debug(f"CDN health: {hostname} ({provider}) — unreachable (ConnectError)")
    except httpx.TimeoutException:
        _mark_unreachable(hostname, provider)
        logger.debug(f"CDN health: {hostname} ({provider}) — unreachable (timeout)")
    except Exception as e:
        logger.debug(f"CDN health: {hostname} ({provider}) — test error: {e}")


def _mark_unreachable(hostname: str, provider: str) -> None:
    with _lock:
        _host_stats[hostname] = _HostStat(
            provider=provider,
            throughput_mbps=0.0,
            latency_ms=0.0,
            is_reachable=False,
        )
