# LESquic

Движок потоковой передачи данных поверх **QUIC / WebTransport** (HTTP/3, extended CONNECT).
Python, asyncio, построен на [aioquic](https://github.com/aiortc/aioquic).

LESquic — это именно *движок*: он даёт сессии, потоки (надёжные, упорядоченные)
и датаграммы (ненадёжные, быстрые). Прикладной протокол — аудио-стриминг,
передача файлов и т.п. — живёт в отдельных проектах поверх него.

## Возможности

- **WebTransportServer** — принимает WebTransport-сессии (extended CONNECT поверх HTTP/3)
- **connect()** — клиент: открывает сессию к серверу
- **Двунаправленные потоки** — `session.create_stream()` / `session.accept_streams()`
- **Однонаправленные потоки** — `session.create_stream(unidirectional=True)`
- **Датаграммы** — `session.send_datagram()` / `session.receive_datagram()`
- Честная фрагментация: 100 КБ+ по одному потоку проходят (проверено в smoke-тесте)
- Совместимость с браузерным API `WebTransport` (Chrome/Edge)

## Установка

```bash
uv venv .venv
uv pip install -p .venv/bin/python aioquic
```

## Быстрый старт

Сертификат для разработки (самоподписанный):

```bash
./scripts/gen_cert.sh
```

Эхо-сервер:

```python
import asyncio
from lesquic import Session, WebTransportServer

async def handler(session: Session):
    async for stream in session.accept_streams():
        while True:
            data = await stream.receive()   # b"" = peer закрыл поток
            if not data:
                break
            await stream.send(data)
        await stream.close()

async def main():
    server = WebTransportServer(handler, certfile="certs/cert.pem", keyfile="certs/key.pem")
    await server.serve("0.0.0.0", 4433)
    await asyncio.Event().wait()

asyncio.run(main())
```

Клиент:

```python
import asyncio
from lesquic import connect

async def main():
    session = await connect("localhost", 4433, verify=False)
    stream = await session.create_stream()
    await stream.send(b"hello")
    print(await stream.receive())          # b"hello"
    await stream.close()
    session.close()

asyncio.run(main())
```

Готовые примеры: `examples/echo_server.py`, `examples/echo_client.py`.

## Структура

```
lesquic/
  __init__.py     — публичный API
  server.py       — WebTransportServer (принимает сессии)
  client.py       — connect() (открывает сессию)
  session.py      — Session: потоки и датаграммы
  stream.py       — SendStream / ReceiveStream / BidirectionalStream
  _protocol.py    — разбор событий aioquic (общий для сервера и клиента)
tests/
  test_smoke.py   — E2E: эхо, параллельные потоки, uni-поток, датаграммы, 100 КБ
examples/         — демо-сервер и клиент
scripts/gen_cert.sh
```

## Проверка

```bash
.venv/bin/python -u tests/test_smoke.py
```

Ожидаемый вывод заканчивается `ALL OK (6/6)`.

## Заметки по реализации

- `session_id` WebTransport-сессии = stream id расширенного CONNECT-запроса.
- aioquic не помечает *локально созданные* bidi-потоки как WebTransport в своём
  H3-состоянии, поэтому ответные данные от пира парсились бы как сырые HTTP/3
  фреймы и терялись. `Session.create_stream()` проставляет
  `frame_type = WEBTRANSPORT_STREAM` вручную — без этого эхо не работает.
- Датаграммы идут через QUIC DATAGRAM (H3_DATAGRAM), лимит `max_datagram_frame_size=65536`.
- Клиент с `verify=False` не проверяет сертификат — только для разработки.
  Браузерный WebTransport требует доверенный сертификат (или флаг
  `chrome://flags/#enable-experimental-web-platform-features` для self-signed).
