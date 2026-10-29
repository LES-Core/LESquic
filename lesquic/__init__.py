"""lesquic — a WebTransport/QUIC streaming engine built on aioquic.

Public API::

    from lesquic import Session, WebTransportServer, connect

Server::

    async def handler(session: Session):
        stream = await anext(session.accept_streams())
        await stream.send(b"hello")

    server = WebTransportServer(handler, certfile=..., keyfile=...)
    await server.serve("0.0.0.0", 4433)

Client::

    session = await connect("localhost", 4433, verify=False)
    stream = await session.create_stream()
    await stream.send(b"ping")
"""

from .client import connect
from .server import WebTransportServer
from .session import Session
from .stream import (
    BidirectionalStream,
    ReceiveStream,
    SendStream,
    SessionClosed,
    StreamClosed,
    StreamReset,
)

__version__ = "0.1.0"

__all__ = [
    "Session",
    "WebTransportServer",
    "connect",
    "BidirectionalStream",
    "ReceiveStream",
    "SendStream",
    "SessionClosed",
    "StreamClosed",
    "StreamReset",
    "__version__",
]
