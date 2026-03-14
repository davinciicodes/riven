from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from loguru import logger
from pydantic import BaseModel, Field

from program.services.downloaders.models import (
    DebridFile,
    InvalidDebridFileException,
    TorrentContainer,
    TorrentFile,
    TorrentInfo,
    UserInfo,
    UnrestrictedLink,
)
from program.settings import settings_manager
from program.utils.request import CircuitBreakerOpen, SmartResponse, SmartSession
from program.media.item import ProcessedItemType

from .shared import DownloaderBase, premium_days_left


class PremiumizeError(Exception):
    """Base exception for Premiumize-related errors."""


class PremiumizeCacheCheckItem(BaseModel):
    cached: bool
    filename: str | None = None
    filesize: int | None = None
    transcoded: bool | None = None


class PremiumizeCacheCheckResponse(BaseModel):
    status: Literal["success", "error"]
    response: list[bool] = Field(default_factory=list)
    filename: list[str | None] = Field(default_factory=list)
    filesize: list[int | None] = Field(default_factory=list)
    transcoded: list[bool | None] = Field(default_factory=list)
    message: str | None = None


class PremiumizeDirectDLContent(BaseModel):
    path: str
    size: int
    link: str
    stream_link: str | None = None
    transcode_status: str | None = None


class PremiumizeDirectDLResponse(BaseModel):
    status: Literal["success", "error"]
    content: list[PremiumizeDirectDLContent] = Field(default_factory=list)
    message: str | None = None


class PremiumizeTransferCreateResponse(BaseModel):
    status: Literal["success", "error"]
    id: str | None = None
    name: str | None = None
    type: str | None = None
    message: str | None = None


class PremiumizeTransfer(BaseModel):
    id: str
    name: str
    status: str  # "waiting", "finished", "seeding", "running", "deleted", "error"
    progress: float = 0.0
    message: str | None = None
    src: str | None = None
    folder_id: str | None = None
    file_id: str | None = None


class PremiumizeTransferListResponse(BaseModel):
    status: Literal["success", "error"]
    transfers: list[PremiumizeTransfer] = Field(default_factory=list)
    message: str | None = None


class PremiumizeAccountInfo(BaseModel):
    status: Literal["success", "error"]
    customer_id: str | None = None
    premium_until: int | None = None  # Unix timestamp
    limit_used: float | None = None
    space_used: int | None = None
    message: str | None = None


class PremiumizeAPI:
    """
    Minimal Premiumize API client using SmartSession for retries, rate limits, and circuit breaker.
    API key is sent as a query param for GET requests and as form data for POST requests.
    """

    BASE_URL = "https://www.premiumize.me/api"

    def __init__(self, api_key: str, proxy_url: str | None = None) -> None:
        self.api_key = api_key
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        self.session = SmartSession(
            base_url=self.BASE_URL,
            rate_limits={
                # Premiumize doesn't publish exact limits; 2 req/s is safe
                "www.premiumize.me": {
                    "rate": 2,
                    "capacity": 20,
                },
            },
            proxies=proxies,
            retries=2,
            backoff_factor=0.5,
        )

    def _get(self, path: str, **params) -> SmartResponse:
        return self.session.get(path, params={"apikey": self.api_key, **params})

    def _post(self, path: str, **data) -> SmartResponse:
        return self.session.post(path, data={"apikey": self.api_key, **data})


class PremiumizeDownloader(DownloaderBase):
    """
    Premiumize downloader.

    Key difference from Real-Debrid/DebridLink: Premiumize has a dedicated cache check
    endpoint (/cache/check) so we never need to add a torrent just to check availability.
    For cached items, /transfer/directdl returns direct CDN URLs immediately.
    """

    def __init__(self) -> None:
        self.key = "premiumize"
        self.settings = settings_manager.settings.downloaders.premiumize
        self.api: PremiumizeAPI | None = None
        self.initialized = self.validate()

    def validate(self) -> bool:
        if not self._validate_settings():
            return False

        proxy_url = self.PROXY_URL or None
        self.api = PremiumizeAPI(api_key=self.settings.api_key, proxy_url=proxy_url)

        return self._validate_premium()

    def _validate_settings(self) -> bool:
        if not self.settings.enabled:
            return False
        if not self.settings.api_key:
            logger.warning("Premiumize API key is not set")
            return False
        return True

    def _validate_premium(self) -> bool:
        try:
            user_info = self.get_user_info()
            if not user_info:
                logger.error("Failed to get Premiumize user info")
                return False
            if user_info.premium_status != "premium":
                logger.error("Premiumize premium membership required")
                return False
            if user_info.premium_expires_at:
                logger.info(premium_days_left(user_info.premium_expires_at))
            return True
        except Exception as e:
            logger.error(f"Failed to validate Premiumize premium status: {e}")
            return False

    def _handle_error(self, response: SmartResponse) -> str:
        status = response.status_code
        if status == 400:
            return "Bad request"
        elif status == 401:
            return "Unauthorized - check API key"
        elif status == 403:
            return "Forbidden"
        elif status == 404:
            return "Not found"
        elif status == 429:
            return "Rate limit exceeded"
        elif status >= 500:
            return "Premiumize server error"
        else:
            try:
                return response.json().get("message", f"HTTP {status}")
            except Exception:
                return f"HTTP {status}"

    def _maybe_backoff(self, response: SmartResponse) -> None:
        if response.status_code == 429 or response.status_code >= 500:
            logger.warning(f"Premiumize rate limit or server error (HTTP {response.status_code}), backing off")
            raise CircuitBreakerOpen("www.premiumize.me")

    def _is_cached(self, infohash: str) -> bool:
        """
        Check if an infohash is instantly available (cached) on Premiumize.
        This endpoint does not cost fair-use quota.
        """
        assert self.api

        response = self.api._get("cache/check", **{"items[]": infohash})
        self._maybe_backoff(response)

        if not response.ok:
            raise PremiumizeError(self._handle_error(response))

        data = PremiumizeCacheCheckResponse.model_validate(response.json())

        if data.status != "success":
            raise PremiumizeError(data.message or "cache/check failed")

        return bool(data.response and data.response[0])

    def _get_directdl(self, infohash: str) -> list[PremiumizeDirectDLContent]:
        """
        Get direct download links for a cached infohash without creating a persistent transfer.
        Returns list of content items with direct CDN links.
        """
        assert self.api

        magnet = f"magnet:?xt=urn:btih:{infohash}"
        response = self.api._post("transfer/directdl", src=magnet)
        self._maybe_backoff(response)

        if not response.ok:
            raise PremiumizeError(self._handle_error(response))

        data = PremiumizeDirectDLResponse.model_validate(response.json())

        if data.status != "success":
            raise PremiumizeError(data.message or "transfer/directdl failed")

        return data.content

    def get_instant_availability(
        self,
        infohash: str,
        item_type: ProcessedItemType,
    ) -> TorrentContainer | None:
        """
        Check cache and, if available, fetch direct download links.

        Unlike Real-Debrid/DebridLink, we use the dedicated cache check endpoint first —
        no need to add the torrent just to probe its status.
        """
        try:
            if not self._is_cached(infohash):
                logger.debug(f"Premiumize: {infohash} is not cached")
                return None

            content = self._get_directdl(infohash)

            if not content:
                logger.debug(f"Premiumize: directdl returned no content for {infohash}")
                return None

            files = list[DebridFile]()
            torrent_files = dict[int, TorrentFile]()

            for idx, item in enumerate(content):
                filename = item.path.split("/")[-1]
                try:
                    df = DebridFile.create(
                        path=item.path,
                        filename=filename,
                        filesize_bytes=item.size,
                        filetype=item_type,
                        file_id=idx,
                    )
                    df.download_url = item.link
                    files.append(df)
                    torrent_files[idx] = TorrentFile(
                        id=idx,
                        path=item.path,
                        bytes=item.size,
                        selected=1,
                        download_url=item.link,
                    )
                except InvalidDebridFileException as e:
                    logger.debug(f"Premiumize {infohash}: {e}")

            if not files:
                logger.debug(f"Premiumize: no valid files for {infohash} after filtering")
                return None

            info = TorrentInfo(
                id=infohash,
                name=infohash,
                status="downloaded",
                infohash=infohash,
                files=torrent_files,
                links=[f.download_url for f in files if f.download_url],
            )

            container = TorrentContainer(infohash=infohash, files=files)
            container.torrent_id = infohash
            container.torrent_info = info

            return container

        except CircuitBreakerOpen:
            logger.debug(f"Circuit breaker OPEN for Premiumize; skipping {infohash}")
            raise
        except PremiumizeError as e:
            logger.warning(f"Premiumize availability check failed [{infohash}]: {e}")
            return None
        except Exception as e:
            logger.debug(f"Premiumize availability check failed [{infohash}]: {e}")
            return None

    def add_torrent(self, infohash: str) -> str:
        """
        Add a torrent by infohash via transfer/create.

        Returns:
            Premiumize transfer ID.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            PremiumizeError: If the API returns a failing status.
        """
        assert self.api

        magnet = f"magnet:?xt=urn:btih:{infohash}"
        response = self.api._post("transfer/create", src=magnet)
        self._maybe_backoff(response)

        if not response.ok:
            raise PremiumizeError(self._handle_error(response))

        data = PremiumizeTransferCreateResponse.model_validate(response.json())

        if data.status != "success" or not data.id:
            raise PremiumizeError(data.message or "transfer/create returned no ID")

        return data.id

    def select_files(self, torrent_id: int | str, file_ids: list[int]) -> None:
        """Premiumize auto-selects all files; no explicit selection needed."""
        pass

    def get_torrent_info(self, torrent_id: int | str) -> TorrentInfo:
        """
        Get transfer info by ID from the transfer list.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            PremiumizeError: If the API returns a failing status or transfer not found.
        """
        assert self.api

        response = self.api._get("transfer/list")
        self._maybe_backoff(response)

        if not response.ok:
            raise PremiumizeError(self._handle_error(response))

        data = PremiumizeTransferListResponse.model_validate(response.json())

        if data.status != "success":
            raise PremiumizeError(data.message or "transfer/list failed")

        transfer = next((t for t in data.transfers if t.id == str(torrent_id)), None)

        if not transfer:
            raise PremiumizeError(f"Transfer {torrent_id} not found in transfer list")

        status = "downloaded" if transfer.status in ("finished", "seeding") else transfer.status

        return TorrentInfo(
            id=torrent_id,
            name=transfer.name,
            status=status,
            infohash=None,
            progress=transfer.progress * 100 if transfer.progress <= 1.0 else transfer.progress,
            files={},
            links=[],
        )

    def delete_torrent(self, torrent_id: int | str) -> None:
        """
        Delete a transfer on Premiumize.

        Raises:
            CircuitBreakerOpen: If the per-domain breaker is OPEN.
            PremiumizeError: If the API returns a failing status.
        """
        assert self.api

        response = self.api._post("transfer/delete", id=str(torrent_id))
        self._maybe_backoff(response)

        if not response.ok:
            raise PremiumizeError(self._handle_error(response))

    def unrestrict_link(self, link: str) -> UnrestrictedLink:
        """
        Premiumize directdl links are already direct CDN URLs — no further unrestriction needed.
        """
        return UnrestrictedLink(
            download=link,
            filename="file",
            filesize=0,
        )

    def get_fresh_cdn_url(self, infohash: str, filename: str) -> str | None:
        """
        Re-call directdl with the infohash to get a fresh CDN URL for an expired file.

        Premiumize CDN URLs are time-limited signed URLs. When one expires (403),
        calling directdl again with the same infohash returns a fresh URL.
        """
        import os
        try:
            content = self._get_directdl(infohash)
            for item in content:
                if item.path and os.path.basename(item.path) == filename:
                    return item.link
            return None
        except Exception as e:
            logger.debug(f"Premiumize get_fresh_cdn_url failed for {filename}: {e}")
            return None

    def get_user_info(self) -> UserInfo | None:
        """
        Get normalized user information from Premiumize.

        Returns:
            UserInfo with normalized fields, or None on error.
        """
        try:
            assert self.api

            response = self.api._get("account/info")
            self._maybe_backoff(response)

            if not response.ok:
                logger.error(f"Premiumize: failed to get user info: {self._handle_error(response)}")
                return None

            data = PremiumizeAccountInfo.model_validate(response.json())

            if data.status != "success":
                logger.error(f"Premiumize: account/info error: {data.message}")
                return None

            premium_expires_at = None
            premium_days_left_val = None
            is_premium = False

            if data.premium_until and data.premium_until > 0:
                premium_expires_at = datetime.fromtimestamp(data.premium_until, tz=timezone.utc)
                now = datetime.now(tz=timezone.utc)
                is_premium = premium_expires_at > now
                if is_premium:
                    premium_days_left_val = max(0, (premium_expires_at - now).days)

            return UserInfo(
                service="premiumize",
                username=None,
                email=None,
                user_id=data.customer_id or "",
                premium_status="premium" if is_premium else "free",
                premium_expires_at=premium_expires_at,
                premium_days_left=premium_days_left_val,
            )

        except Exception as e:
            logger.error(f"Premiumize: error getting user info: {e}")
            return None
