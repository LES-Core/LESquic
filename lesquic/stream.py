"""Stream primitives for lesquic WebTransport sessions."""

from __future__ import annotations

import asyncio
from typing import Any, Optional

__all__ = [
    "SessionClosed",
    "StreamClosed",
    "StreamReset",
    "ReceiveStream",
    "SendStream",
    "BidirectionalStream",
]

_END = object()


class StreamClosed(Exception):
    """Raised when operating on a stream that has already ended."""


class SessionClosed(Exception):
    """Raised when the WebTransport session (or connection) is closed."""


class StreamReset(Exception):
    """Raised when the peer resets the stream (QUIC RESET_STREAM)."""

    def __init__(self, error_code: int) -> None:
        super().__init__(f"stream reset by peer (error_code={error_code})")
        self.error_code = error_code


class ReceiveStream:
    """The readable half of a WebTransport stream.

    :meth:`receive` returns the next chunk of bytes, or ``b""`` once the
    peer has finished the stream (FIN). A peer reset raises
    :class:`StreamReset`.
    """

    def __init__(self, stream_id: int) -> None:
        self.stream_id = stream_id
        self._queue: asyncio.Queue = asyncio.Queue()
        self._ended = False
        self.reset_code: Optional[int] = None

    @property
    def ended(self) -> bool:
        return self._ended

    # -- internal feed API (used by the protocol layer) --------------------

    def _feed(self, data: bytes, ended: bool) -> None:
        if self._ended:
            return
        if data:
            self._queue.put_nowait(data)
        if ended:
            self._ended = True
            self._queue.put_nowait(_END)

    def _feed_reset(self, error_code: int) -> None:
        if self._ended:
            return
        self._ended = True
        self.reset_code = error_code
        self._queue.put_nowait(("reset", error_code))

    def _feed_eof(self) -> None:
        """Mark the stream as finished without more data (connection gone)."""
        if self._ended:
            return
        self._ended = True
        self._queue.put_nowait(_END)

    # -- public API ---------------------------------------------------------

    async def receive(self) -> bytes:
        """Return the next data chunk; ``b""`` means end-of-stream."""
        item: Any = await self._queue.get()
        if item is _END:
            return b""
        if isinstance(item, tuple) and item and item[0] == "reset":
            raise StreamReset(item[1])
        return bytes(item)


class SendStream:
    """The writable half of a WebTransport stream."""

    def __init__(self, stream_id: int, protocol: Any) -> None:
        self.stream_id = stream_id
        self._protocol = protocol
        self._finished = False

    @property
    def finished(self) -> bool:
        return self._finished

    async def send(self, data: bytes) -> None:
        """Send bytes on the stream.

        aioquic buffers data internally and applies QUIC flow control, so
        this returns as soon as the payload is queued for transmission.
        """
        if self._finished:
            raise StreamClosed(f"stream {self.stream_id} is closed")
        self._protocol._quic.send_stream_data(self.stream_id, data)
        self._protocol.transmit()

    async def close(self) -> None:
        """Finish the stream cleanly (sends FIN)."""
        if self._finished:
            return
        self._finished = True
        self._protocol._quic.send_stream_data(self.stream_id, b"", end_stream=True)
        self._protocol.transmit()

    def reset(self, error_code: int = 0) -> None:
        """Abort the stream with a RESET_STREAM frame."""
        self._finished = True
        self._protocol._quic.reset_stream(self.stream_id, error_code=error_code)
        self._protocol.transmit()


class BidirectionalStream(SendStream, ReceiveStream):
    """A bidirectional WebTransport stream (both halves)."""

    def __init__(self, stream_id: int, protocol: Any) -> None:
        SendStream.__init__(self, stream_id, protocol)
        ReceiveStream.__init__(self, stream_id)
