"""Echo client demo for lesquic.

Run examples/echo_server.py first, then:
    .venv/bin/python examples/echo_client.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lesquic import connect


async def main() -> None:
    session = await connect("127.0.0.1", 4433, verify=False)
    print(f"connected, session_id={session.session_id}")

    stream = await session.create_stream()
    for i in range(3):
        msg = f"ping-{i}".encode()
        await stream.send(msg)
        reply = await stream.receive()
        print(f"sent {msg!r} -> got {reply!r}")
    await stream.close()

    await session.send_datagram(b"hello-datagram")
    reply = await session.receive_datagram()
    print(f"datagram -> {reply!r}")

    session.close()
    print("done")


if __name__ == "__main__":
    asyncio.run(main())
