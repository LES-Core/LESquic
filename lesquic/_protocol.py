"""QUIC/HTTP3 protocol plumbing for lesquic (shared by server and client)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

from aioquic.asyncio import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import (
    DatagramReceived,
    HeadersReceived,
    WebTransportStreamDataReceived,
)
from aioquic.quic.events import (
    ConnectionTerminated,
    QuicEvent,
    StreamDataReceived,
    StreamReset,
)

from .session import Session
from .stream import BidirectionalStream, ReceiveStream

logger = logging.getLogger("lesquic")

SessionHandler = Callable[[Session], Awaitable[None]]


def _is_client_initiated_bidi(stream_id: int) -> bool:
    return stream_id & 0x3 == 0x0


def _is_unidirectional(stream_id: int) -> bool:
    return stream_id & 0x3 in (0x2, 0x3)


class WebTransportProtocol(QuicConnectionProtocol):
    """Drives one QUIC connection as a WebTransport endpoint."""

    def __init__(
        self,
        *args: Any,
        session_handler: Optional[SessionHandler] = None,
        path: str = "/",
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._h3: Optional[H3Connection] = None
        self._session: Optional[Session] = None
        self._session_handler = session_handler
        self._session_path = path
        self._session_ready: asyncio.Future[Session] = asyncio.get_running_loop().create_future()

    @property
    def session(self) -> Optional[Session]:
        return self._session

    # -- helpers --------------------------------------------------------------

    def _ensure_h3(self) -> H3Connection:
        if self._h3 is None:
            self._h3 = H3Connection(self._quic, enable_webtransport=True)
        return self._h3

    def _make_session(self, session_id: int, path: str) -> Session:
        h3 = self._ensure_h3()
        peer = self._quic.get_peer_address()
        session = Session(h3=h3, protocol=self, session_id=session_id, path=path, peer=peer)
        self._session = session
        if not self._session_ready.done():
            self._session_ready.set_result(session)
        return session

    # -- aioquic hooks ----------------------------------------------------------

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, ConnectionTerminated):
            logger.debug(
                "connection terminated: code=%s reason=%r",
                event.error_code,
                event.reason_phrase,
            )
            if self._session is not None:
                self._session._feed_closed()
            if not self._session_ready.done():
                self._session_ready.set_exception(
                    ConnectionError("connection terminated before session")
                )
            return

        h3 = self._ensure_h3()
        for h3_event in h3.handle_event(event):
            if isinstance(h3_event, HeadersReceived):
                self._on_headers(h3_event, h3)
            elif isinstance(h3_event, WebTransportStreamDataReceived):
                self._on_stream_data(h3_event)
            elif isinstance(h3_event, DatagramReceived):
                if self._session is not None and h3_event.stream_id == self._session.session_id:
                    self._session._feed_datagram(h3_event.data)
        # StreamReset is a QUIC-level event: feed the receive half
        if isinstance(event, StreamReset):
            if self._session is not None:
                stream = self._session._streams.get(event.stream_id)
                if isinstance(stream, (BidirectionalStream, ReceiveStream)):
                    stream._feed_reset(event.error_code)

    # -- header / handshake handling (subclass-specific) -----------------------

    def _on_headers(self, event: HeadersReceived, h3: H3Connection) -> None:
        raise NotImplementedError

    def _on_stream_data(self, event: WebTransportStreamDataReceived) -> None:
        assert self._session is not None
        stream_id = event.stream_id
        if stream_id == self._session.session_id:
            return  # data on the CONNECT stream itself: ignore
        if not _is_unidirectional(stream_id) and not _is_client_initiated_bidi(stream_id):
            # server-initiated bidi streams also arrive as remote streams
            pass
        stream = self._session._register_remote_stream(
            stream_id, unidirectional=_is_unidirectional(stream_id)
        )
        if isinstance(stream, (BidirectionalStream, ReceiveStream)):
            stream._feed(event.data, event.stream_ended)
        self.transmit()
