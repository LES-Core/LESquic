"""WebTransport session abstraction for lesquic."""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Optional, Union

from aioquic.h3.connection import FrameType

from .stream import (
    BidirectionalStream,
    ReceiveStream,
    SendStream,
    SessionClosed,
)

__all__ = ["Session"]


class Session:
    """A WebTransport session (an HTTP/3 CONNECT request over QUIC).

    The engine creates one ``Session`` per client connection and hands it
    to the user's ``session_handler``. On the client side the session is
    returned by :func:`lesquic.connect`.

    Data paths available on a session:

    * **bidirectional streams** — ordered, reliable, both directions
    * **unidirectional streams** — ordered, reliable, one direction
    * **datagrams** — unordered, unreliable (QUIC DATAGRAM frames)
    """

    def __init__(
        self,
        protocol: Any,
        h3: Any,
        session_id: int,
        *,
        path: str = "/",
        peer: Optional[Any] = None,
    ) -> None:
        self.session_id = session_id
        self.path = path
        self.peer = peer
        self._protocol = protocol
        self._h3 = h3
        self._closed = False
        self._incoming: asyncio.Queue[
            Optional[Union[BidirectionalStream, ReceiveStream]]
        ] = asyncio.Queue()
        self._datagrams: asyncio.Queue[Optional[bytes]] = asyncio.Queue()
        self._streams: dict[int, Union[BidirectionalStream, ReceiveStream, SendStream]] = {}

    # -- state ---------------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._closed

    # -- streams -------------------------------------------------------------

    async def create_stream(self, unidirectional: bool = False) -> Union[BidirectionalStream, SendStream]:
        """Open a new WebTransport stream.

        Returns a :class:`BidirectionalStream` by default, or a
        :class:`SendStream` (write half only) for unidirectional streams.
        """
        self._check_open()
        stream_id = self._h3.create_webtransport_stream(
            self.session_id, is_unidirectional=unidirectional
        )
        if not unidirectional:
            # aioquic does NOT mark client-initiated bidi WT streams in its
            # local H3 state, so data echoed back by the peer would be parsed
            # as raw HTTP/3 frames and silently swallowed. Mark it ourselves
            # so receive-side emits WebTransportStreamDataReceived.
            with self._h3._get_or_create_stream(stream_id) as h3stream:
                h3stream.frame_type = FrameType.WEBTRANSPORT_STREAM
                h3stream.session_id = self.session_id
        stream: Union[BidirectionalStream, SendStream]
        if unidirectional:
            stream = SendStream(stream_id, self._protocol)
        else:
            stream = BidirectionalStream(stream_id, self._protocol)
        self._streams[stream_id] = stream
        self._protocol.transmit()
        return stream

    async def accept_streams(self) -> AsyncIterator[Union[BidirectionalStream, ReceiveStream]]:
        """Async iterator over streams opened by the peer."""
        while True:
            item = await self._incoming.get()
            if item is None:  # sentinel: session closed
                return
            yield item

    # -- datagrams -----------------------------------------------------------

    async def send_datagram(self, data: bytes) -> None:
        """Send an unreliable datagram (QUIC DATAGRAM frame)."""
        self._check_open()
        self._h3.send_datagram(stream_id=self.session_id, data=data)
        self._protocol.transmit()

    async def receive_datagram(self) -> bytes:
        """Receive the next datagram sent by the peer (waits)."""
        item = await self._datagrams.get()
        if item is None:
            raise SessionClosed("session closed")
        return item

    # -- lifecycle -----------------------------------------------------------

    def close(self, error_code: int = 0) -> None:
        """Close the underlying QUIC connection."""
        if self._closed:
            return
        self._closed = True
        try:
            self._protocol._quic.close(error_code=error_code)
        except Exception:
            pass
        self._protocol.transmit()

    # -- internal feed API (protocol layer) ----------------------------------

    def _feed_incoming(self, stream: Union[BidirectionalStream, ReceiveStream]) -> None:
        self._incoming.put_nowait(stream)

    def _feed_datagram(self, data: bytes) -> None:
        self._datagrams.put_nowait(data)

    def _feed_closed(self) -> None:
        """Wake up waiters when the session dies."""
        if self._closed:
            return
        self._closed = True
        self._incoming.put_nowait(None)
        self._datagrams.put_nowait(None)
        for stream in self._streams.values():
            if hasattr(stream, "_feed_eof"):
                stream._feed_eof()  # type: ignore[attr-defined]

    def _register_remote_stream(
        self, stream_id: int, unidirectional: bool
    ) -> Union[BidirectionalStream, ReceiveStream]:
        """Get-or-create the object for a peer-initiated stream."""
        existing = self._streams.get(stream_id)
        if isinstance(existing, (BidirectionalStream, ReceiveStream)):
            return existing
        if unidirectional:
            stream: Union[BidirectionalStream, ReceiveStream] = ReceiveStream(stream_id)
        else:
            stream = BidirectionalStream(stream_id, self._protocol)
        self._streams[stream_id] = stream
        self._incoming.put_nowait(stream)
        return stream

    def _check_open(self) -> None:
        if self._closed:
            raise SessionClosed("session is closed")
