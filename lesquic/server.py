"""WebTransport server for lesquic."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Awaitable, Callable, Optional, Union

from aioquic.h3.connection import H3_ALPN
from aioquic.h3.events import HeadersReceived
from aioquic.quic.configuration import QuicConfiguration

from ._protocol import WebTransportProtocol
from .session import Session

logger = logging.getLogger("lesquic")

SessionHandler = Callable[[Session], Awaitable[None]]

__all__ = ["WebTransportServer"]


def _header_dict(headers: list) -> dict:
    return {k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v for k, v in headers}


class _ServerProtocol(WebTransportProtocol):
    """Accepts an incoming extended CONNECT (webtransport) request."""

    def __init__(self, *args, session_handler: Optional[SessionHandler] = None, **kwargs):
        super().__init__(*args, session_handler=session_handler, **kwargs)
        self._session_handler_task: Optional[asyncio.Task] = None

    def _on_headers(self, event: HeadersReceived, h3) -> None:
        headers = _header_dict(event.headers)
        # validate: this must be an extended CONNECT for webtransport
        if (
            headers.get(":method") != "CONNECT"
            or headers.get(":protocol") != "webtransport"
        ):
            h3.send_headers(
                stream_id=event.stream_id,
                headers=[(b":status", b"400")],
            )
            self.transmit()
            return

        path = headers.get(":path", "/")
        session = self._make_session(session_id=event.stream_id, path=path)
        logger.info("webtransport session opened: path=%s peer=%s", path, session.peer)

        # accept the session
        h3.send_headers(
            stream_id=event.stream_id,
            headers=[
                (b":status", b"200"),
                (b"sec-webtransport-http3-draft", b"draft02"),
            ],
        )
        self.transmit()

        if self._session_handler is not None:
            self._session_handler_task = asyncio.ensure_future(
                self._run_handler(session)
            )

    async def _run_handler(self, session: Session) -> None:
        try:
            await self._session_handler(session)  # type: ignore[misc]
        except Exception:
            logger.exception("session handler crashed")
        finally:
            if not session.closed:
                session.close()

    def connection_made(self, transport) -> None:  # noqa: D102
        super().connection_made(transport)

    def connection_lost(self, exc) -> None:
        if self._session is not None:
            self._session._feed_closed()
        super().connection_lost(exc)


class WebTransportServer:
    """A QUIC/WebTransport server.

    Usage::

        async def handler(session: Session):
            async for stream in session.accept_streams():
                ...

        server = WebTransportServer(handler, certfile="certs/cert.pem",
                                    keyfile="certs/key.pem")
        await server.serve(host="0.0.0.0", port=4433)
    """

    def __init__(
        self,
        session_handler: SessionHandler,
        *,
        certfile: Union[str, Path],
        keyfile: Union[str, Path],
        congestion_control_algorithm: str = "reno",
        max_datagram_frame_size: int = 65536,
    ) -> None:
        self._session_handler = session_handler
        self._configuration = QuicConfiguration(
            is_client=False,
            alpn_protocols=H3_ALPN,
            max_datagram_frame_size=max_datagram_frame_size,
            congestion_control_algorithm=congestion_control_algorithm,
        )
        self._configuration.load_cert_chain(str(certfile), str(keyfile))
        self._server = None

    async def serve(self, host: str = "0.0.0.0", port: int = 4433) -> None:
        """Start listening (non-blocking; the server runs until cancelled)."""
        from aioquic.asyncio import serve

        handler = self._session_handler

        def create_protocol(*args, **kwargs):
            return _ServerProtocol(
                *args, session_handler=handler, **kwargs
            )

        self._server = await serve(
            host,
            port,
            configuration=self._configuration,
            create_protocol=create_protocol,
        )
        logger.info("lesquic server listening on %s:%s/udp", host, port)

    def close(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
            logger.info("lesquic server closed")
