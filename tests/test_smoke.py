"""End-to-end smoke test: lesquic echo server + client over localhost."""

import asyncio
import sys

sys.path.insert(0, ".")

from lesquic import Session, WebTransportServer, connect


async def echo_handler(session: Session) -> None:
    print(f"[server] session opened, path={session.path}")

    async def datagram_loop() -> None:
        while True:
            dg = await session.receive_datagram()
            await session.send_datagram(b"dg-echo:" + dg)

    dg_task = asyncio.ensure_future(datagram_loop())

    async def stream_loop() -> None:
        async for stream in session.accept_streams():
            kind = type(stream).__name__
            print(f"[server] stream accepted: id={stream.stream_id} ({kind})")
            if hasattr(stream, "send"):  # bidirectional -> raw echo
                while True:
                    data = await stream.receive()
                    if not data:
                        break
                    await stream.send(data)
                await stream.close()
                print(f"[server] stream {stream.stream_id} echoed & closed")
            else:  # unidirectional (receive-only) -> drain
                total = 0
                while True:
                    data = await stream.receive()
                    if not data:
                        break
                    total += len(data)
                print(f"[server] uni stream drained: {total} bytes")

    sl_task = asyncio.ensure_future(stream_loop())
    try:
        await asyncio.gather(dg_task, sl_task)
    except Exception:
        pass
    finally:
        dg_task.cancel()
        sl_task.cancel()


async def main() -> int:
    server = WebTransportServer(
        echo_handler, certfile="certs/cert.pem", keyfile="certs/key.pem"
    )
    await server.serve("127.0.0.1", 4433)

    ok = 0
    try:
        session = await connect("127.0.0.1", 4433, path="/echo", verify=False)
        print(f"[client] connected, session_id={session.session_id}")

        # --- 1. bidirectional echo ---
        stream = await session.create_stream()
        await stream.send(b"hello lesquic")
        reply = await stream.receive()
        assert reply == b"hello lesquic", reply
        print("[client] bidi echo OK")
        ok += 1

        for i in range(5):
            await stream.send(f"msg-{i}".encode())
            reply = await stream.receive()
            assert reply == f"msg-{i}".encode(), reply
        print("[client] 5 sequential messages OK")
        ok += 1
        await stream.close()

        # --- 2. second concurrent stream ---
        stream2 = await session.create_stream()
        await stream2.send(b"second stream")
        reply = await stream2.receive()
        assert reply == b"second stream", reply
        print("[client] second stream OK")
        ok += 1
        await stream2.close()

        # --- 3. unidirectional stream (client -> server) ---
        uni = await session.create_stream(unidirectional=True)
        await uni.send(b"one-way data")
        await uni.close()
        await asyncio.sleep(0.3)
        print("[client] unidirectional stream OK")
        ok += 1

        # --- 4. datagram round-trip ---
        await session.send_datagram(b"dg-ping")
        reply = await asyncio.wait_for(session.receive_datagram(), timeout=5)
        assert reply == b"dg-echo:dg-ping", reply
        print(f"[client] datagram round-trip OK: {reply!r}")
        ok += 1

        # --- 5. big transfer with fragmentation: half-close then read ---
        payload = bytes(range(256)) * 400  # 100 KB
        stream3 = await session.create_stream()
        await stream3.send(payload)
        await stream3.close()  # FIN -> server finishes echoing and closes
        buf = b""
        while True:
            chunk = await asyncio.wait_for(stream3.receive(), timeout=15)
            if not chunk:
                break
            buf += chunk
        assert buf == payload, (len(buf), len(payload))
        print(f"[client] 100KB round-trip OK ({len(buf)} bytes)")
        ok += 1

        session.close()
        await asyncio.sleep(0.3)
    finally:
        server.close()

    print(f"ALL OK ({ok}/6)")
    return 0 if ok == 6 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
