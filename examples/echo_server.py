"""Echo server demo for lesquic.

Run:
    .venv/bin/python examples/echo_server.py

Then run examples/echo_client.py in another terminal.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lesquic import Session, WebTransportServer


async def handler(session: Session) -> None:
    print(f"[session] opened path={session.path} peer={session.peer}")

    async def streams() -> None:
        async for stream in session.accept_streams():
            print(f"[session] stream {stream.stream_id} ({type(stream).__name__})")
            while True:
                data = await stream.receive()
                if not data:
                    break
                if hasattr(stream, "send"):
                    await stream.send(data)  # echo
            if hasattr(stream, "close"):
                await stream.close()

    async def datagrams() -> None:
        while True:
            dg = await session.receive_datagram()
            print(f"[session] datagram <- {dg!r}")
            await session.send_datagram(b"echo:" + dg)

    try:
        await asyncio.gather(streams(), datagrams())
    except Exception:
        pass
    print(f"[session] closed path={session.path}")


async def main() -> None:
    root = Path(__file__).resolve().parent.parent
    server = WebTransportServer(
        handler,
        certfile=root / "certs/cert.pem",
        keyfile=root / "certs/key.pem",
    )
    await server.serve("127.0.0.1", 4433)
    print("lesquic echo server on 127.0.0.1:4433/udp — Ctrl+C to stop")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        server.close()


if __name__ == "__main__":
    asyncio.run(main())
