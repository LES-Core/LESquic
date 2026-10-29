"""WebTransport client for lesquic."""

from __future__ import annotations

import asyncio
import logging
import socket
from typing import Optional

from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection

from ._protocol import WebTransportProtocol
from .session import Session

logger = logging.getLogger("lesquic")

__all__ = ["connect"]


def _header_dict(headers: list) -> dict:
    return {k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v for k, v in headers}


class _ClientProtocol(WebTransportProtocol):
    """Initiates an extended CONNECT (webtransport) request."""

    def __init__(self, *args, authority: str = "localhost", path: str = "/", **kwargs):
        super().__init__(*args, path=path, **kwargs)
        self._authority = authority
        self._connect_stream_id: Optional[int] = None

    def initiate(self) -> None:
        """Send the extended CONNECT to open a session."""
        h3 = self._ensure_h3()
        stream_id = self._quic.get_next_available_stream_id()
        self._connect_stream_id = stream_id
        h3.send_headers(
            stream_id=stream_id,
            headers=[
                (b":method", b"CONNECT"),
                (b":scheme", b"https"),
                (b":authority", self._authority.encode()),
                (b":path", self._session_path.encode()),
                (b":protocol", b"webtransport"),
            ],
        )
        # register the session object now: its id is the CONNECT stream id
        self._make_session(session_id=stream_id, path=self._session_path)
        self.transmit()

    def _on_headers(self, event: HeadersReceived, h3: H3Connection) -> None:
        headers = _header_dict(event.headers)
        status = headers.get(":status", "")
        if event.stream_id == self._connect_stream_id:
            if status == "200":
                logger.info("webtransport session accepted by server")
            else:
                logger.warning("server rejected webtransport session: %s", status)
                if self._session is not None:
                    self._session._feed_closed()
                if not self._session_ready.done():
                    self._session_ready.set_exception(
                        ConnectionError(f"session rejected: HTTP {status}")
                    )


class _Connection:
    """Owns the client transport + protocol, mirrors aioquic's connect()."""

    def __init__(self, protocol: _ClientProtocol, transport, closed: asyncio.Event):
        self.protocol = protocol
        self._transport = transport
        self._closed = closed

    async def wait_closed(self) -> None:
        await self._closed.wait()

    def close(self) -> None:
        self.protocol.close()
        self._transport.close()


async def connect(
    host: str,
    port: int,
    *,
    path: str = "/",
    cafile: Optional[str] = None,
    verify: bool = True,
    congestion_control_algorithm: str = "reno",
    max_datagram_frame_size: int = 65536,
    timeout: float = 10.0,
) -> Session:
    """Connect to a WebTransport server and return the :class:`Session`.

    ``verify=False`` skips certificate validation — handy for self-signed
    certs during development (browsers require trusting the cert too).
    """
    loop = asyncio.get_running_loop()

    configuration = QuicConfiguration(
        is_client=True,
        alpn_protocols=H3_ALPN,
        max_datagram_frame_size=max_datagram_frame_size,
        congestion_control_algorithm=congestion_control_algorithm,
        verify_mode=None if verify else 0,  # 0 = ssl.CERT_NONE
    )
    configuration.server_name = host
    if cafile:
        configuration.load_verify_locations(cafile=cafile)

    # dual-stack UDP socket, like aioquic's own client
    local_host = "::"
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_DGRAM)
    addr = infos[0][4]
    if len(addr) == 2:
        addr = ("::ffff:" + addr[0], addr[1], 0, 0)

    quic = QuicConnection(configuration=configuration)
    authority = f"{host}:{port}"

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: _ClientProtocol(
            quic,
            authority=authority,
            path=path,
            session_handler=None,
        ),
        local_addr=(local_host, 0),
    )
    closed = asyncio.Event()
    original_lost = protocol.connection_lost

    def connection_lost(exc):
        closed.set()
        original_lost(exc)

    protocol.connection_lost = connection_lost  # type: ignore[method-assign]

    try:
        protocol.connect(addr, transmit=True)
        await asyncio.wait_for(protocol.wait_connected(), timeout=timeout)
        protocol.initiate()
        session = await asyncio.wait_for(protocol._session_ready, timeout=timeout)
    except BaseException:
        transport.close()
        raise

    # attach the connection so the caller can close the transport later
    session._connection = _Connection(protocol, transport, closed)  # type: ignore[attr-defined]
    return session
