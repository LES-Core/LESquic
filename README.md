<div align="center">

<img src="icon.png" width="325px">

# ⚡ LESquic

**Движок потоковой передачи данных поверх QUIC / WebTransport**

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![HTTP/3 + WebTransport](https://img.shields.io/badge/Protocol-QUIC_·_WebTransport-22c55e?style=for-the-badge&logo=http&logoColor=white)](https://www.rfc-editor.org/rfc/rfc9000)
[![Built on aioquic](https://img.shields.io/badge/Built_on-aioquic-0A9EDC?style=for-the-badge&logo=python&logoColor=white)](https://github.com/aiortc/aioquic)
[![asyncio](https://img.shields.io/badge/Async-asyncio_native-FFD43B?style=for-the-badge&logo=python&logoColor=black)](https://docs.python.org/3/library/asyncio.html)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue?style=for-the-badge&logo=apache&logoColor=white)](https://www.apache.org/licenses/LICENSE-2.0)

Сессии · Надёжные стримы · Ненадёжные датаграммы · Один движок

</div>

---

## 📋 Содержание

- [📖 О проекте](#-о-проекте)
- [🤔 Зачем WebTransport](#-зачем-webtransport)
- [✨ Возможности](#-возможности)
- [🚀 Быстрый старт](#-быстрый-старт)
- [📚 API](#-api)
- [🌐 Браузерный клиент](#-браузерный-клиент)
- [🧪 Тестирование](#-тестирование)
- [🏗️ Архитектура](#️-архитектура)
- [📁 Структура проекта](#-структура-проекта)
- [📄 Лицензия](#-лицензия)

---

## 📖 О проекте

**LESquic** — это движок реального времени на Python, который даёт приложениям
современный транспорт вместо устаревших TCP-сокетов и WebSocket:

- **QUIC** (RFC 9000) — протокол поверх UDP с шифрованием TLS 1.3, мультиплексированием
  потоков без head-of-line blocking и мгновенным reconnect без нового handshake
- **WebTransport** — стандарт W3C, который открывает этот протокол браузерам
  и совместим с нативными клиентами

LESquic — **транспортный слой, а не готовое приложение**. Он не знает ничего
о том, *что* вы передаёте: аудио, видео, игровые состояния, метрики или файлы —
прикладной протокол вы строите сами на трёх примитивах движка:
**сессии, стримы и датаграммы**.

> Движок построен поверх проверенной библиотеки [aioquic](https://github.com/aiortc/aioquic)
> (та же реализация QUIC, что используется в aiortc) и добавляет поверх неё
> удобный asyncio-native API уровня WebTransport-сессии.

## 🤔 Зачем WebTransport

| | TCP-сокеты | WebSocket | **WebTransport (LESquic)** |
|---|---|---|---|
| Шифрование | ❌ нет | ✅ TLS | ✅ TLS 1.3 из коробки |
| Независимые потоки | ❌ | ❌ head-of-line blocking | ✅ каждый стрим независим |
| Ненадёжная доставка (датаграммы) | ❌ | ❌ | ✅ для live-данных |
| Доступ из браузера | ❌ | ✅ | ✅ нативный WebTransport API |
| 0-RTT reconnect | ❌ | ❌ | ✅ |
| Мультиплексирование | одно соединение = один поток | одно соединение, фреймы | ✅ много стримов в одном соединении |

**Когда это полезно:** live-стриминг медиа, многопользовательские игры,
телеметрия и IoT, интерактивные чаты, передача больших файлов с прогрессом —
всё, где важны низкая задержка и параллельные каналы данных.

## ✨ Возможности

- 🖥️ **Сервер** — `WebTransportServer` принимает WebTransport-сессии
  (extended CONNECT поверх HTTP/3), одна сессия на соединение, изолированные хендлеры
- 🔌 **Клиент** — `connect()` открывает сессию к любому WebTransport-серверу
  (включая серверы других реализаций)
- 🔀 **Двунаправленные стримы** — надёжные упорядоченные каналы с asyncio API:
  `send()` / `receive()` / `close()`
- ➡️ **Однонаправленные стримы** — передача только в одну сторону
  (идеально для «толкания» данных: сервер → клиент без ответа)
- 📨 **Датаграммы** — ненадёжная доставка без ретрансмиссий для live-данных,
  где свежие важнее полных
- 📦 **Честная фрагментация** — передача сотен килобайт одним стримом
  работает из коробки (проверено тестами)
- 🌐 **Совместимость с браузерами** — протокольная часть соответствует
  браузерному API `WebTransport`, к серверу можно подключаться из Chrome/Edge
- 🧩 **Чистая абстракция** — никакой магии: события aioquic преобразуются
  в понятные объекты `Session` / `Stream` / датаграммы

## 🚀 Быстрый старт

### Требования

- Python 3.10+
- Linux / macOS / Windows (всё, где работает aioquic)

### Установка

```bash
git clone https://github.com/LES-Core/LESquic.git
cd LESquic
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Сертификат

WebTransport работает только поверх TLS. Для разработки сгенерируйте
самоподписанный сертификат:

```bash
./scripts/gen_cert.sh
```

Для продакшена используйте сертификат от доверенного CA (например, Let's Encrypt).

### Эхо-сервер за 30 секунд

```python
import asyncio
from lesquic import Session, WebTransportServer

async def handler(session: Session):
    """Вызывается для каждой подключившейся сессии."""
    async for stream in session.accept_streams():
        while True:
            data = await stream.receive()   # b"" — пир закрыл стрим
            if not data:
                break
            await stream.send(data)         # эхо
        await stream.close()

async def main():
    server = WebTransportServer(
        handler, certfile="certs/cert.pem", keyfile="certs/key.pem"
    )
    await server.serve("0.0.0.0", 4433)
    await asyncio.Event().wait()            # работать вечно

asyncio.run(main())
```

### Клиент

```python
import asyncio
from lesquic import connect

async def main():
    session = await connect("localhost", 4433, verify=False)  # verify=False — только для dev

    # надёжный стрим
    stream = await session.create_stream()
    await stream.send(b"hello")
    print(await stream.receive())           # b"hello"
    await stream.close()

    # ненадёжная датаграмма
    await session.send_datagram(b"live-payload")

    session.close()

asyncio.run(main())
```

Готовые примеры: [`examples/echo_server.py`](examples/echo_server.py)
и [`examples/echo_client.py`](examples/echo_client.py) — запустите сервер,
затем клиент, и посмотрите, как данные идут по обоим путям.

## 📚 API

### `WebTransportServer(session_handler, *, certfile, keyfile, ...)`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `session_handler` | — | `async def handler(session)` для каждой новой сессии |
| `certfile` / `keyfile` | — | PEM-сертификат и приватный ключ |
| `congestion_control_algorithm` | `"reno"` | алгоритм управления перегрузкой QUIC |
| `max_datagram_frame_size` | `65536` | максимальный размер датаграммы |

- `await server.serve(host, port)` — начать слушать (не блокирует)
- `server.close()` — остановить и закрыть все соединения

### `connect(host, port, *, path="/", verify=True, cafile=None, timeout=10.0) -> Session`

Подключается к серверу, выполняет handshake, открывает WebTransport-сессию
и возвращает `Session`. Бросает `ConnectionError`, если сервер отклонил сессию.

### `Session`

| Метод / свойство | Описание |
|---|---|
| `await session.create_stream(unidirectional=False)` | открыть стрим: `BidirectionalStream` или `SendStream` |
| `async for stream in session.accept_streams()` | принимать стримы, открытые пиром |
| `await session.send_datagram(data)` | отправить датаграмму |
| `await session.receive_datagram()` | принять датаграмму (ожидает) |
| `session.path` | путь из запроса сессии (например, `/echo`) |
| `session.peer` | адрес пира `(ip, port)` |
| `session.closed` | закрыта ли сессия |
| `session.close()` | закрыть соединение |

### Стримы

| Класс | Умеет |
|---|---|
| `BidirectionalStream` | `send(data)` · `receive()` · `close()` · `reset(code)` |
| `SendStream` (uni) | `send(data)` · `close()` · `reset(code)` |
| `ReceiveStream` (uni, принятый от пира) | `receive()` |

- `receive()` возвращает `bytes`; пустой результат `b""` = пир корректно закрыл стрим (FIN)
- если пир сбросил стрим, поднимается `StreamReset(error_code)`
- операции над закрытым стримом поднимают `StreamClosed`, над закрытой сессией — `SessionClosed`

### Модель данных

```
Соединение (QUIC + TLS 1.3)
└── Сессия (WebTransport, extended CONNECT)
    ├── Bidi-стримы  — надёжно, упорядоченно, в обе стороны
    ├── Uni-стримы   — надёжно, упорядоченно, в одну сторону
    └── Датаграммы   — ненадёжно, без порядка, мгновенно
```

**Правило выбора:** нужен гарантированный порядок (файл, команда, чанк медиа) —
стрим; нужна свежесть любой ценой (позиции в игре, live-кадры) — датаграмма.

## 🌐 Браузерный клиент

Сервер LESquic совместим со стандартным браузерным API
[WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport):

```js
const transport = new WebTransport("https://localhost:4433/echo");
await transport.ready;

// надёжный стрим
const stream = await transport.createBidirectionalStream();
const writer = stream.writable.getWriter();
await writer.write(new TextEncoder().encode("hello"));

// датаграмма
const dg = transport.datagrams;
dg.writable.getWriter().write(new TextEncoder().encode("live"));
```

| Браузер | Поддержка |
|---|---|
| Chrome / Edge 97+ | ✅ |
| Firefox | 🚩 за флагом `network.webtransport.webtransport-http3` |
| Safari | ❌ пока нет |

⚠️ Браузеры требуют сертификат от доверенного CA. Для разработки с
самоподписанным сертификатом включите
`chrome://flags/#enable-experimental-web-platform-features`
или подключайтесь нативным клиентом `connect(..., verify=False)`.

## 🧪 Тестирование

Полный E2E-тест (сервер + клиент в одном процессе, через реальную сеть):

```bash
.venv/bin/python -u tests/test_smoke.py
```

Проверяется: эхо по bidi-стриму, последовательные сообщения, параллельные
стримы, однонаправленный стрим, round-trip датаграмм и фрагментированная
передача 100 КБ. Ожидаемый результат — `ALL OK (6/6)`.

Тот же тест автоматически гоняется в CI при каждом пуше, затрагивающем код.

## 🏗️ Архитектура

```
┌──────────────────────────────────────────────┐
│                ваше приложение                │
├──────────────────────────────────────────────┤
│  lesquic                                      │
│  ┌────────────┐  ┌───────────┐  ┌──────────┐ │
│  │  Session   │  │  Streams  │  │Datagrams │ │  ← публичный API
│  └─────┬──────┘  └─────┬─────┘  └────┬─────┘ │
│        └───────┬───────┴─────────────┘       │
│         ┌──────▼───────┐                     │
│         │ _protocol.py │  ← разбор событий   │
│         └──────┬───────┘                     │
├────────────────┼─────────────────────────────┤
│         ┌──────▼───────┐                     │
│         │   aioquic    │  HTTP/3 + QUIC      │
│         └──────┬───────┘                     │
│         ┌──────▼───────┐                     │
│         │  UDP / TLS   │                     │
│         └──────────────┘                     │
└──────────────────────────────────────────────┘
```

- `server.py` / `client.py` — жизненный цикл соединений (asyncio endpoints)
- `_protocol.py` — общий драйвер: преобразует события QUIC/H3 в объекты движка
- `session.py` — сессия: роутинг стримов и датаграмм, жизненный цикл
- `stream.py` — потоковые примитивы с очередями asyncio

### Известные тонкости реализации

- `session_id` WebTransport-сессии совпадает со stream id расширенного
  CONNECT-запроса — так устроен протокол (draft), движок следует ему
- aioquic не помечает локально созданные bidi-стримы как WebTransport в своём
  H3-состоянии — LESquic проставляет `frame_type=WEBTRANSPORT_STREAM` вручную
  в `Session.create_stream()`, иначе ответные данные терялись бы при разборе
- датаграммы используют QUIC DATAGRAM (RFC 9221) с H3_DATAGRAM,
  размер ограничен `max_datagram_frame_size`

## 📁 Структура проекта

```
LESquic/
├── lesquic/               # движок
│   ├── __init__.py       # публичный API
│   ├── server.py         # WebTransportServer
│   ├── client.py         # connect()
│   ├── session.py        # Session
│   ├── stream.py         # SendStream / ReceiveStream / BidirectionalStream
│   └── _protocol.py      # драйвер событий aioquic
├── examples/             # эхо-сервер и эхо-клиент
├── tests/                # E2E smoke-тест
├── scripts/gen_cert.sh   # самоподписанный dev-сертификат
├── .github/workflows/    # CI: smoke-тест на каждый пуш
├── requirements.txt
└── README.md
```

## 📄 Лицензия

<div align="center">

Данный проект распространяется под лицензией **Apache-2.0**.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue?style=for-the-badge&logo=apache&logoColor=white)](https://www.apache.org/licenses/LICENSE-2.0)

**© 2026 LESquic**

</div>

---

## 🧩 Другие проекты

<div align="center">

С другими проектами можете ознакомиться в организации **LES-Core**:

👉 **[github.com/LES-Core](https://github.com/LES-Core)**

</div>

---

<div align="center">

### ⭐ Если вам понравился проект, поставьте звёздочку на GitHub!

</div>
