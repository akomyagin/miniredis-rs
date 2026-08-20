# miniredis-rs — TECHNICAL_PLAN

Детальный технический план: стек, ключевые решения, разбивка по Этапам.

## Стек и ключевые решения

| Решение | Выбор | Обоснование |
|---|---|---|
| Язык / edition | Rust 2021, toolchain ≥ 1.96 | второй Rust-проект в портфеле; edition 2021 — самый обкатанный |
| Асинхронность | **Нет `tokio` в v1** — только `std::net` | цель проекта — вручную прочувствовать потоковый TCP и синхронизацию разделяемого состояния; async-рантайм спрятал бы ровно эту механику за своими абстракциями |
| Конкурентная модель | **thread-per-connection** (`std::thread::spawn` на соединение) | простейшая корректная модель для учебного KV; операционные системы тянут сотни-тысячи потоков, а профиль нагрузки pet-проекта — десятки соединений. Переход на async — задокументированный POST_MVP-пункт, не MVP-зависимость |
| Разделяемое хранилище | `Arc<Mutex<Store>>` в v1 | одна блокировка проще и корректнее; шардирование/`RwLock` — оптимизация, к которой возвращаемся только если `redis-benchmark` покажет, что мьютекс — узкое место (см. Этап 5) |
| LRU за O(1) | двусвязный список поверх slab-арены (`Vec<Node>` + `usize`-индексы, без `unsafe`), вручную | реализовано в Этапе 4; см. раздел ниже — решение и trade-off (безопасная альтернатива intrusive-list с сырыми указателями) |
| Версия протокола | RESP2 | достаточно для PING/GET/SET/DEL/EXPIRE/TTL и совместимо с `redis-cli`; RESP3 — за скоупом |
| Внешние зависимости MVP | нулевые | самодостаточный бинарь на всех пяти Этапах, никакого Docker Compose, тест голым `redis-cli`; крейт `lru` как запасной путь не понадобился |

### Почему `std::net`, а не `tokio` (зафиксировано)

Главная учебная ценность — потоковый парсинг и синхронизация. `tokio` дал бы `AsyncRead`,
который сам буферизует и абстрагирует фрагментацию, — то есть спрятал бы главную мину проекта.
Thread-per-connection оставляет `read()` голым: обработчик обязан сам буферизовать и
возобновлять парсинг на границе фрагмента. Это дороже по памяти на соединение, но именно то,
что мы хотим прочувствовать. Если понадобится масштаб на 10k+ соединений — миграция
`src/resp.rs` на `tokio` вынесена в POST_MVP.

## Разбивка по Этапам

Каждый Этап — отдельная ветка от `master` (`stage/N-...`) → PR → merge (см. `CLAUDE.md`).

Ниже — план реализационного уровня детализации: точные файлы, функции, сигнатуры, алгоритмы,
таблицы обработки ошибок и тест-кейсы для Этапов 1–5, достаточные, чтобы агент-кодер без
контекста прошлых обсуждений мог реализовать любой Этап напрямую. Все сигнатуры, уже объявленные
в заглушках Этапа 0 (`Value`, `ParseError`, `Parser`, `Entry`, `Store`), сохраняются как
контракт — последующие Этапы их реализуют, а не переопределяют.

### Этап 0 — Скелет (готов)

- `cargo init`, `Cargo.toml` с нулевыми рантайм-зависимостями и зафиксированным решением по
  конкурентности в комментарии.
- Модули-заглушки: `src/resp.rs` (типы `Value`/`ParseError`/`Parser` + `encode`, все с
  `TODO(Этап N)`), `src/store.rs` (`Entry`/`Store` с `TODO(Этап N)`).
- `src/main.rs` биндит TCP и принимает соединения (accept-and-drop).
- **Критерий готовности:** `cargo check` и `cargo build` проходят чисто.

### Этап 1 — RESP-кодек (протокольный слой)

Сердце проекта. RESP2-типы: `+` simple, `-` error, `:` integer, `$` bulk, `*` array; терминатор
кадра — CRLF. Клиентская команда приходит как `*`-массив bulk-строк.

#### Файлы и типы

Всё в `src/resp.rs`. Состав вариантов `Value`/`ParseError` не менять:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    /// Null bulk string (`$-1\r\n`) — единственный null-вариант; на входе `*-1\r\n`
    /// (null array) декодируется в тот же вариант, различие RESP2 между null bulk и
    /// null array на уровне `Value` не сохраняется.
    Null,
    Array(Vec<Value>),
}

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Protocol(String),
}

#[derive(Default)]
pub struct Parser {
    buf: Vec<u8>,
    // Отдельный курсор потребления не хранится: try_parse парсит с начала buf и при
    // успехе делает buf.drain(..consumed), так что buf всегда начинается с границы
    // следующего непарсенного кадра.
}

impl Parser {
    pub fn new() -> Self { Self::default() }
    pub fn feed(&mut self, bytes: &[u8]) { self.buf.extend_from_slice(bytes); }
    pub fn try_parse(&mut self) -> Result<Value, ParseError> { /* см. алгоритм ниже */ }
}

pub fn encode(value: &Value) -> Vec<u8> { /* см. ниже */ }
```

Приватный помощник: `fn parse_value(buf: &[u8]) -> Result<(Value, usize), ParseError>`, где
`usize` — число потреблённых байт от начала `buf`. `try_parse` вызывает его на `&self.buf`:
при `Ok((value, consumed))` делает `self.buf.drain(..consumed)` и возвращает `Ok(value)`; при
`Err(Incomplete)` или `Err(Protocol(_))` буфер не трогает (протокольная ошибка — сигнал для
вызывающего кода закрыть соединение, не пытаясь ресинхронизироваться на побитом потоке).

#### Алгоритм `parse_value`

Работает на срезе `buf`, ничего не мутирует. Пустой `buf` → сразу `Err(Incomplete)`.

Общий помощник: `fn find_crlf(buf: &[u8], from: usize) -> Option<usize>` — первое вхождение
`"\r\n"` начиная с `from` (`buf[from..].windows(2).position(|w| w == b"\r\n").map(|i| from + i)`).
Отсутствие → недостаточно данных, `Incomplete`.

Диспетчер по первому байту `buf[0]`: `+`/`-`/`:`/`$`/`*` — как ниже; иначе
`Err(Protocol("invalid type byte"))`.

1. **Simple String `+...\r\n`:** CRLF от позиции 1 (нет → `Incomplete`). Срез `buf[1..crlf]`
   декодируется как UTF-8; невалидный UTF-8 → `Protocol("simple string is not valid UTF-8")`
   (simple strings по конвенции RESP не бинарны — только короткие ASCII-статусы вроде `+OK`;
   бинарные данные идут через bulk). Успех → `(Value::Simple(s), crlf + 2)`.
2. **Error `-...\r\n`:** идентично Simple String, вариант `Value::Error`.
3. **Integer `:...\r\n`:** CRLF от позиции 1 (нет → `Incomplete`). `buf[1..crlf]` парсится как
   `i64`; ошибка парсинга → `Protocol("invalid integer")`. Успех → `(Value::Integer(n), crlf+2)`.
4. **Bulk String `$<len>\r\n<len bytes>\r\n`:** CRLF от позиции 1 — конец длины (нет →
   `Incomplete`). `buf[1..crlf]` парсится как `i64` (не `usize` напрямую — нужно отличить
   легитимный `-1` от битой длины):
   - ошибка парсинга → `Protocol("invalid bulk length")`;
   - `len == -1` → null bulk, `(Value::Null, crlf + 2)`, тела нет;
   - `len < -1` → `Protocol("negative bulk length")`;
   - `len > 512 * 1024 * 1024` (512 MiB, порог как `proto-max-bulk-len` по умолчанию в
     настоящем Redis) → `Protocol("bulk length exceeds limit")`;
   - иначе `n = len as usize`. Тело начинается с `body_start = crlf + 2`, нужно
     `body_start + n + 2` байт всего (тело + хвостовой CRLF). Не хватает → `Incomplete`.
     Хвост не равен `\r\n` → `Protocol("bulk string missing trailing CRLF")`. Успех →
     `(Value::Bulk(buf[body_start..body_start+n].to_vec()), body_start + n + 2)`.
5. **Array `*<count>\r\n<count elements>`:** CRLF от позиции 1 (нет → `Incomplete`).
   `buf[1..crlf]` парсится как `i64`:
   - ошибка парсинга → `Protocol("invalid array length")`;
   - `count == -1` → null array, представлен тем же `Value::Null` — `(Value::Null, crlf+2)`;
   - `count < -1` → `Protocol("negative array length")`;
   - `count > 1_048_576` → `Protocol("array length exceeds limit")`;
   - иначе рекурсивно парсим `count` элементов подряд начиная с `pos = crlf + 2`,
     аккумулируя `consumed`; `Incomplete` на любом элементе пробрасывается без потребления
     ничего; `Protocol` пробрасывается как есть. Успех → `(Value::Array(items), pos)`.
   Явный предел глубины рекурсии не вводится (не путать с лимитом длины — тот обязателен как
   защита от OOM на некорректной длине, этого достаточно).

#### Кодировщик `encode`

Точное зеркало декодера:

```rust
pub fn encode(value: &Value) -> Vec<u8> {
    match value {
        Value::Simple(s) => format!("+{s}\r\n").into_bytes(),
        Value::Error(s)  => format!("-{s}\r\n").into_bytes(),
        Value::Integer(n) => format!(":{n}\r\n").into_bytes(),
        Value::Bulk(b) => {
            let mut out = format!("${}\r\n", b.len()).into_bytes();
            out.extend_from_slice(b);
            out.extend_from_slice(b"\r\n");
            out
        }
        Value::Null => b"$-1\r\n".to_vec(),
        Value::Array(items) => {
            let mut out = format!("*{}\r\n", items.len()).into_bytes();
            for item in items {
                out.extend_from_slice(&encode(item));
            }
            out
        }
    }
}
```

`Value::Null` на выходе всегда кодируется как `$-1\r\n` (null bulk), никогда как `*-1\r\n` —
сознательное упрощение энкодера, не открытый вопрос.

#### Обработка ошибок (сводка)

| Вход | Результат |
|---|---|
| Пустой буфер / обрыв на границе кадра | `Err(Incomplete)`, буфер не потребляется |
| Неизвестный первый байт | `Err(Protocol(..))` |
| Simple/Error с невалидным UTF-8 | `Err(Protocol(..))` |
| `:` с нечисловым содержимым | `Err(Protocol(..))` |
| `$` с нечисловой длиной / длиной `< -1` / `> 512 MiB` | `Err(Protocol(..))` |
| `$` без хвостового `\r\n` после тела | `Err(Protocol(..))` |
| `$-1\r\n` | `Ok(Value::Null)` |
| `*` с нечисловым count / count `< -1` / `> 1_048_576` | `Err(Protocol(..))` |
| `*-1\r\n` | `Ok(Value::Null)` |
| Вложенный элемент дал `Incomplete`/`Protocol` | пробрасывается наверх как есть |

Ни один путь не паникует (обязательное требование SKILL.md).

#### Тестовый план

Юнит-тесты в `src/resp.rs`, `#[cfg(test)] mod tests`, минимум 22 кейса:

**Happy path / round-trip:** `encode` для каждого варианта `Value` (включая пустую bulk-строку
`Bulk(vec![])` → `$0\r\n\r\n`, отличая от `Null`); round-trip `decode(encode(v)) == v` для всех
вариантов, включая вложенный `Array` и bulk с не-UTF8 байтами.

**Фрагментация — обязательный набор (SKILL.md):**

1. `parses_byte_by_byte` — скормить кадр в `Parser` по одному байту через `feed`, `try_parse`
   после каждого; все вызовы кроме последнего → `Incomplete`, последний → корректный `Value`.
2. `parses_random_chunks` — разбить кадр на чанки случайной длины (детерминированный
   ручной ГСЧ без внешних крейтов — `dev-dependencies` пуст, добавлять `rand` не нужно;
   например LCG: `seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1)`), прогнать
   50–100 разбиений, сверить итоговый `Value` с эталоном на каждом.
3. `parses_multiple_frames_in_one_chunk` — два кадра одним `feed()`, два последовательных
   `try_parse()` отдают каждый кадр по порядку, третий вызов → `Incomplete`.
4. `feed_across_frame_boundary_mixed` — чанк 1 = целый первый кадр + начало второго; чанк 2 =
   остаток второго; корректная последовательность `try_parse()`/`feed()`.
5. `property_split_invariant` — хелпер `feed_fragmented(bytes, split_points) -> Value`,
   исчерпывающий перебор всех точек разреза на 2 части (`for i in 1..bytes.len()`) для одного
   сложного вложенного кадра (~30–50 байт) — результат идентичен на любом разбиении.

**Ошибки протокола:** неизвестный тип-байт; нечисловая длина bulk/array; отрицательная длина
`< -1`; оборванный хвостовой CRLF bulk (сначала `Incomplete` пока байт не хватает, затем
`Protocol` при полном, но неверном хвосте); нечисловой `:`; невалидный UTF-8 в Simple string;
`$-1\r\n` и `*1\r\n$-1\r\n` — корректный `Null`, не ошибка; длина bulk `> 512 MiB` →
`Protocol` до попытки аллокации.

#### Критерий готовности

- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` чисты.
- Round-trip `encode ∘ decode` подтверждён тестом для всех вариантов `Value`.
- Ни один тест с некорректным вводом не паникует.
- Проверка живым `redis-cli` не имеет смысла на этом Этапе — TCP-цикл появляется в Этапе 2;
  готовность подтверждается юнит-тестами.

#### Открытые решения

Нет — алгоритм полностью детерминирован выше. Тексты сообщений `Protocol(String)` не
фиксируются как контракт, кроме случаев, которые Этап 2 прокидывает клиенту как `-ERR ...`.

### Этап 2 — Базовые команды над `HashMap`+`Mutex`

#### Файлы и типы

`src/store.rs`:

```rust
use std::collections::HashMap;
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at: Option<Instant>,
}

#[derive(Default)]
pub struct Store {
    map: HashMap<Vec<u8>, Entry>,
}

impl Store {
    pub fn new() -> Self { Self::default() }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        self.map.insert(key, Entry { value, expires_at });
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        self.map.remove(key).is_some()
    }

    pub fn sweep_expired(&mut self) {
        // TODO(Этап 3): iterate and drop entries whose expires_at is in the past.
    }
}
```

На этом Этапе `expires_at` во всех вызовах `set` из диспатча — всегда `None`; поле уже
присутствует в `Entry` как задел под Этап 3, логика истечения не реализуется.

Новый файл `src/command.rs` (регистрируется в `main.rs` как `mod command;`) — командный слой,
третий поверх RESP-кодека и хранилища (`store.rs` намеренно не знает о `Value`/RESP — «слой
данных, ничего не знает о сети», см. PLAN.md; `main.rs` — точка входа/сетевой цикл; диспатч не
подходит ни туда, ни туда):

```rust
use crate::resp::Value;
use crate::store::Store;
use std::sync::{Arc, Mutex};

/// Диспатчит один разобранный клиентский фрейм (обязан быть Value::Array of Value::Bulk)
/// на разделяемый store, возвращая RESP-ответ для записи обратно.
pub fn dispatch(store: &Arc<Mutex<Store>>, frame: Value) -> Value {
    let args = match extract_args(frame) {
        Ok(args) => args,
        Err(e) => return Value::Error(e),
    };
    if args.is_empty() {
        return Value::Error("ERR empty command".into());
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match name.as_str() {
        "PING" => cmd_ping(&args),
        "SET" => cmd_set(store, &args),
        "GET" => cmd_get(store, &args),
        "DEL" => cmd_del(store, &args),
        _ => Value::Error(format!("ERR unknown command '{name}'")),
    }
}

/// Клиентский фрейм обязан быть Array of Bulk (так шлют команды redis-cli и реальные
/// клиенты). Что-то другое — протокольная ошибка использования.
fn extract_args(frame: Value) -> Result<Vec<Vec<u8>>, String> {
    match frame {
        Value::Array(items) => items
            .into_iter()
            .map(|v| match v {
                Value::Bulk(b) => Ok(b),
                _ => Err("ERR protocol error: expected bulk string array element".to_string()),
            })
            .collect(),
        _ => Err("ERR protocol error: expected array of bulk strings".to_string()),
    }
}

fn cmd_ping(args: &[Vec<u8>]) -> Value {
    match args.len() {
        1 => Value::Simple("PONG".into()),
        2 => Value::Bulk(args[1].clone()),
        _ => Value::Error("ERR wrong number of arguments for 'ping' command".into()),
    }
}

fn cmd_set(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 3 {
        return Value::Error("ERR wrong number of arguments for 'set' command".into());
    }
    let key = args[1].clone();
    let value = args[2].clone();
    store.lock().unwrap().set(key, value, None);
    Value::Simple("OK".into())
}

fn cmd_get(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 2 {
        return Value::Error("ERR wrong number of arguments for 'get' command".into());
    }
    match store.lock().unwrap().get(&args[1]) {
        Some(v) => Value::Bulk(v),
        None => Value::Null,
    }
}

fn cmd_del(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() < 2 {
        return Value::Error("ERR wrong number of arguments for 'del' command".into());
    }
    let mut store = store.lock().unwrap();
    let count = args[1..].iter().filter(|k| store.del(k)).count();
    Value::Integer(count as i64)
}
```

`src/main.rs`: снять `#![allow(dead_code)]`, если всё используется (иначе оставить, если что-то
по-прежнему не задействовано до Этапа 3 — сверить `cargo clippy`).

```rust
mod command;
mod resp;
mod store;

use resp::{ParseError, Parser};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use store::Store;

const DEFAULT_ADDR: &str = "127.0.0.1:6379";

fn main() -> std::io::Result<()> {
    let addr = std::env::args().nth(1).unwrap_or_else(|| DEFAULT_ADDR.to_string());
    let listener = TcpListener::bind(&addr)?;
    println!("miniredis listening on {addr}");

    let store = Arc::new(Mutex::new(Store::new()));

    for stream in listener.incoming() {
        let stream = stream?;
        let store = Arc::clone(&store);
        std::thread::spawn(move || handle_connection(stream, store));
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    let mut parser = Parser::new();
    let mut read_buf = [0u8; 4096];

    loop {
        // Вычерпать все уже буферизованные полные кадры до следующего read().
        loop {
            match parser.try_parse() {
                Ok(frame) => {
                    let reply = command::dispatch(&store, frame);
                    if stream.write_all(&resp::encode(&reply)).is_err() {
                        return; // клиент отвалился
                    }
                }
                Err(ParseError::Incomplete) => break,
                Err(ParseError::Protocol(msg)) => {
                    let _ = stream
                        .write_all(&resp::encode(&resp::Value::Error(format!("ERR {msg}"))));
                    return; // протокольный десинхрон — небезопасно продолжать парсинг
                }
            }
        }

        match stream.read(&mut read_buf) {
            Ok(0) => return,          // клиент закрыл соединение
            Ok(n) => parser.feed(&read_buf[..n]),
            Err(_) => return,          // ошибка чтения — обрываем соединение
        }
    }
}
```

Ключевой момент: после каждого `read()` парсер вычерпывается **полностью** прежде чем снова
блокироваться на `read()` — корректно обрабатывает «несколько кадров в одном TCP-пакете»
(pipeline-режим `redis-cli`, `redis-benchmark`).

#### Обработка ошибок

- Неизвестная команда → `-ERR unknown command '<NAME>'`.
- Неверное число аргументов → `-ERR wrong number of arguments for '<cmd>' command`.
- Верхнеуровневый фрейм — не `Array`, либо элемент — не `Bulk` → `-ERR protocol error: expected
  array of bulk strings`.
- `ParseError::Protocol` из кодека → `-ERR <msg>`, соединение закрывается (не пытаемся
  ресинхронизироваться на побитом потоке — осознанное решение по объёму MVP).
- Паника внутри одного обработчика не роняет сервер целиком (гарантия thread-per-connection),
  но код всё равно не должен паниковать намеренно на кривом вводе — `.unwrap()` допустим только
  на `Mutex::lock()` (poisoning — см. Этап 5), не на данных из сети.

#### Тестовый план

Юнит-тесты `src/command.rs` (`Store` + `command::dispatch`, без сети): PING без/с аргументом;
SET→GET round-trip; GET отсутствующего ключа → `Null`; DEL существующего+отсутствующего ключа;
DEL нескольких ключей; неверная арность SET/GET → `Error`; неизвестная команда → `Error`;
регистронезависимость команды (`sEt`); нефрейм-массив на входе dispatch → `Error` без паники;
перезапись существующего ключа; бинарные (не-UTF8) значения проходят через `Vec<u8>`
неизменными.

Юнит-тесты `src/store.rs`: `get`/`set`/`del` напрямую на `Store` без RESP-слоя.

Интеграционный фейк, детерминированный, без сети (`tests/integration_fake.rs` либо секция в
`command.rs` — расположение на усмотрение реализатора): прогнать закодированные RESP-команды
через реальный `Parser` + `command::dispatch` + `encode` как если бы это были байты из сокета
(`SET foo bar` → `+OK\r\n`, `GET foo` → `$3\r\nbar\r\n`, `DEL foo` → `:1\r\n`); тот же сценарий с
фрагментированной подачей байт (переиспользовать `feed_fragmented` из Этапа 1) — подтверждает,
что связка кодек+диспатч тоже переживает дробление на границе кадра.

#### Критерий готовности

- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` чисты.
- Живой `redis-cli`:
  ```
  redis-cli -p 6379 ping                  # PONG
  redis-cli -p 6379 set foo bar           # OK
  redis-cli -p 6379 get foo               # "bar"
  redis-cli -p 6379 del foo               # (integer) 1
  redis-cli -p 6379 get foo               # (nil)
  ```
  Плюс: два одновременных `redis-cli`-клиента видят один и тот же `Store` (SET в одном, GET в
  другом).

#### Открытые решения

Расположение интеграционного фейка (`tests/` vs inline `#[cfg(test)]`) — на усмотрение
реализатора. Точные тексты `-ERR ...` сверх зафиксированных выше — не контракт.

### Этап 3 — TTL

#### Файлы и типы

`src/store.rs` — дополнить существующие методы, не переименовывая:

```rust
impl Store {
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_expired(key) {
            self.map.remove(key);
            return None;
        }
        self.map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        self.map.insert(key, Entry { value, expires_at });
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        self.map.remove(key).is_some()
    }

    /// Поддержка TTL: секунды до истечения, семантика Redis.
    ///   -2 => ключа нет (или уже истёк, трактуется как отсутствующий)
    ///   -1 => ключ есть, но TTL не задан
    ///    n => секунд осталось (округление вверх — как Redis, никогда не занижает: ключ
    ///         с 400мс до истечения репортит 1, не 0)
    pub fn ttl_secs(&mut self, key: &[u8]) -> i64 {
        if self.is_expired(key) {
            self.map.remove(key);
            return -2;
        }
        match self.map.get(key) {
            None => -2,
            Some(Entry { expires_at: None, .. }) => -1,
            Some(Entry { expires_at: Some(at), .. }) => {
                let now = Instant::now();
                if *at <= now {
                    -2 // не должно случаться (is_expired перехватил бы раньше), защита
                } else {
                    let remaining = at.duration_since(now);
                    let secs = remaining.as_secs() as i64;
                    let has_subsecond = remaining.subsec_nanos() > 0;
                    if has_subsecond { secs + 1 } else { secs.max(1) }
                }
            }
        }
    }

    /// EXPIRE: установить/перезаписать TTL существующего ключа. false, если ключа нет
    /// (или уже истёк).
    pub fn expire(&mut self, key: &[u8], expires_at: Instant) -> bool {
        if self.is_expired(key) {
            self.map.remove(key);
            return false;
        }
        match self.map.get_mut(key) {
            Some(entry) => { entry.expires_at = Some(expires_at); true }
            None => false,
        }
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        match self.map.get(key) {
            Some(Entry { expires_at: Some(at), .. }) => *at <= Instant::now(),
            _ => false,
        }
    }

    /// Реап всех истёкших на текущий момент ключей. Вызывается фоновым sweeper-потоком.
    pub fn sweep_expired(&mut self) {
        let now = Instant::now();
        self.map.retain(|_, entry| match entry.expires_at {
            Some(at) => at > now,
            None => true,
        });
    }
}
```

`ttl_secs` и `expire` — новые публичные методы `Store` (план фиксирует точные имена/сигнатуры;
TECHNICAL_PLAN.md уже допускал «тонкий хелпер на `Store`, если так чище» — это он и есть).
Округление `ttl_secs` вверх выбрано для соответствия поведению настоящего Redis и стабильности
тестов по времени в CI.

`src/command.rs` — новые ветки диспатча и разбор опций `SET`:

```rust
"SET" => cmd_set(store, &args),
"EXPIRE" => cmd_expire(store, &args),
"TTL" => cmd_ttl(store, &args),
```

`cmd_set` переписывается под опциональные `EX seconds`/`PX millis`:

```rust
fn cmd_set(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() < 3 {
        return Value::Error("ERR wrong number of arguments for 'set' command".into());
    }
    let key = args[1].clone();
    let value = args[2].clone();

    let expires_at = match parse_set_expiry(&args[3..]) {
        Ok(exp) => exp,
        Err(e) => return Value::Error(e),
    };

    store.lock().unwrap().set(key, value, expires_at);
    Value::Simple("OK".into())
}

/// Разбирает хвостовые опции `SET key value [EX seconds | PX milliseconds]`. MVP
/// поддерживает не более одной из EX/PX, никаких других опций (NX/XX/KEEPTTL — вне
/// скоупа, см. «Открытые решения»).
fn parse_set_expiry(opts: &[Vec<u8>]) -> Result<Option<Instant>, String> {
    if opts.is_empty() {
        return Ok(None);
    }
    if opts.len() != 2 {
        return Err("ERR syntax error".into());
    }
    let opt_name = String::from_utf8_lossy(&opts[0]).to_ascii_uppercase();
    let n: i64 = std::str::from_utf8(&opts[1])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())?;
    if n <= 0 {
        return Err("ERR invalid expire time in 'set' command".into());
    }
    match opt_name.as_str() {
        "EX" => Ok(Some(Instant::now() + std::time::Duration::from_secs(n as u64))),
        "PX" => Ok(Some(Instant::now() + std::time::Duration::from_millis(n as u64))),
        _ => Err("ERR syntax error".into()),
    }
}

fn cmd_expire(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 3 {
        return Value::Error("ERR wrong number of arguments for 'expire' command".into());
    }
    let secs: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return Value::Error("ERR value is not an integer or out of range".into()),
    };
    if secs <= 0 {
        // Семантика Redis: неположительный expire немедленно удаляет ключ.
        let existed = store.lock().unwrap().del(&args[1]);
        return Value::Integer(if existed { 1 } else { 0 });
    }
    let at = Instant::now() + std::time::Duration::from_secs(secs as u64);
    let ok = store.lock().unwrap().expire(&args[1], at);
    Value::Integer(if ok { 1 } else { 0 })
}

fn cmd_ttl(store: &Arc<Mutex<Store>>, args: &[Vec<u8>]) -> Value {
    if args.len() != 2 {
        return Value::Error("ERR wrong number of arguments for 'ttl' command".into());
    }
    Value::Integer(store.lock().unwrap().ttl_secs(&args[1]))
}
```

`src/main.rs` — фоновый sweeper, запускается один раз в `main()`:

```rust
const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

fn spawn_sweeper(store: Arc<Mutex<Store>>) {
    std::thread::spawn(move || loop {
        std::thread::sleep(SWEEP_INTERVAL);
        store.lock().unwrap().sweep_expired();
    });
}
```

Вызывается из `main()` сразу после создания `store`, до цикла `listener.incoming()`. Поток —
фоновый, не джойнится; процесс завершается по Ctrl-C/сигналу, как и остальные потоки (нет
graceful shutdown ни у одного из них — см. «Открытые решения» Этапа 5).

#### Обработка ошибок

- `SET key value EX abc` → `-ERR value is not an integer or out of range`.
- `SET key value EX 0` / `EX -5` → `-ERR invalid expire time in 'set' command` (`SET`
  отвергает неположительный `EX`/`PX`, в отличие от `EXPIRE`, где неположительное значение
  означает немедленное удаление — разное поведение двух команд, зафиксировано умышленно).
- `SET key value FOO 5` (неизвестная опция) → `-ERR syntax error`.
- `SET key value EX` (не хватает значения) → `-ERR syntax error`.
- `EXPIRE key abc` → `-ERR value is not an integer or out of range`.
- `EXPIRE missingkey 10` → `Integer(0)` (не ошибка — конвенция Redis).
- `TTL missingkey` → `Integer(-2)`; `TTL keywithoutexpiry` → `Integer(-1)`.

#### Тестовый план

Юнит-тесты `src/store.rs`: `set` без TTL → `ttl_secs == -1`; `get` отсутствующего ключа →
`ttl_secs == -2`; `set` с будущим expiry → `get` возвращает значение; `get` истёкшего ключа →
`None`, ключ физически удалён (использовать `Instant::now()` как `expires_at` + короткий
`sleep`, не вычитание из `Instant`, чтобы не зависеть от гарантий отсутствия паники при
антипереполнении на всех платформах); `expire` на существующем/отсутствующем ключе;
`sweep_expired` удаляет только реально истёкшие (три ключа: без TTL, с TTL в будущем, с TTL в
прошлом); `ttl_secs` округляет вверх (`1500ms` → `2`).

Юнит-тесты `src/command.rs`: `SET ... EX 10` → `TTL` в `1..=10`; `SET ... PX 5000` → `TTL` в
`1..=5`; `EX 0`/нечисловой `EX`/неизвестная опция → `Error`; `EXPIRE` на отсутствующем ключе →
`0`; `EXPIRE`+`TTL` round-trip; `EXPIRE key 0` удаляет ключ; `TTL` на ключе без/с
несуществующим ключом → `-1`/`-2`.

Тайминг-зависимые тесты (интеграционный фейк, `tests/integration_fake.rs` или отдельный файл):
ключ истекает через полный стек RESP после `sleep` за пределами `SET ... PX`; ключ истекает
через `sweep_expired()` **без** обращения через `GET` (для однозначной диагностики — добавить
тестовый геттер `#[cfg(test)] fn len(&self) -> usize { self.map.len() }`, чтобы убедиться, что
именно `sweep_expired()` уменьшил число записей, а не последующий ленивый доступ). Держать
таймауты с запасом (200–300мс), чтобы не флапать в CI.

#### Критерий готовности

- `cargo test` (включая тайминговые тесты без флапа при 3–5 повторных прогонах), `cargo clippy
  -- -D warnings`, `cargo fmt --check` чисты.
- Живой `redis-cli`:
  ```
  redis-cli -p 6379 set foo bar ex 1
  redis-cli -p 6379 ttl foo        # (integer) 1
  sleep 1.5
  redis-cli -p 6379 get foo        # (nil)
  redis-cli -p 6379 set bar baz
  redis-cli -p 6379 expire bar 100
  redis-cli -p 6379 ttl bar        # (integer) ~100
  ```
- Наблюдаемо, что ключ с истёкшим TTL исчезает даже без явного `GET` — фоновый sweeper реально
  работает, не только ленивая экспирация.

#### Открытые решения

Интервал sweeper'а (100 мс) уже зафиксирован текстом плана, не открытый вопрос. Опции `SET`
`NX`/`XX`/`KEEPTTL` сознательно вне скоупа MVP — не реализуются, падают в `_ => Err("ERR syntax
error")` как неизвестная опция.

### Этап 4 — LRU-вытеснение O(1)

**Основной путь — slab-арена**, зафиксированная этим планом как реализация по умолчанию
(соответствует «по умолчанию пишем сами» выше). Запасной путь — крейт `lru` — остаётся
допустимым **только** если arena окажется непропорционально дорогой по времени; переход
требует явной записи причины в этом файле, Этап 4, до мержа.

#### Файлы и типы

`src/store.rs` — приватный модуль LRU-арены прямо в файле (логика инкапсулирована внутри
`Store`, не пересекает границу модуля наружу):

```rust
const NIL: usize = usize::MAX; // сентинел «нет узла» вместо Option<usize> в горячих полях —
                                 // usize::MAX никогда не валидный индекс арены.

/// Один слот в LRU-арене. `prev`/`next` — индексы в `LruList::nodes`, либо `NIL`.
struct Node {
    key: Vec<u8>,       // дублированный ключ, чтобы eviction мог удалить его из HashMap
    prev: usize,
    next: usize,
}

/// Двусвязный список поверх Vec-арены: O(1) touch (move-to-front) и O(1) eviction
/// (сброс хвоста = least-recently-used), без `unsafe` и сырых указателей — индексы
/// вместо ссылок.
struct LruList {
    nodes: Vec<Node>,
    head: usize, // most-recently-used; NIL если пусто
    tail: usize, // least-recently-used; NIL если пусто
    free: Vec<usize>, // переиспользуемые индексы слотов
}

impl LruList {
    fn new() -> Self {
        Self { nodes: Vec::new(), head: NIL, tail: NIL, free: Vec::new() }
    }

    /// Вставляет новый узел для `key` в голову (most-recently-used). Возвращает индекс
    /// слота — сохранить в Entry для O(1) будущих touch/remove.
    fn push_front(&mut self, key: Vec<u8>) -> usize {
        let idx = match self.free.pop() {
            Some(i) => { self.nodes[i] = Node { key, prev: NIL, next: self.head }; i }
            None => { self.nodes.push(Node { key, prev: NIL, next: self.head }); self.nodes.len() - 1 }
        };
        if self.head != NIL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
        idx
    }

    /// Отвязывает `idx` откуда бы он ни был, привязывает в голову. O(1).
    fn move_to_front(&mut self, idx: usize) {
        if self.head == idx {
            return; // уже MRU
        }
        self.unlink(idx);
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = self.head;
        if self.head != NIL {
            self.nodes[self.head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
    }

    /// Удаляет `idx` из списка, освобождает слот для переиспользования. O(1).
    fn remove(&mut self, idx: usize) {
        self.unlink(idx);
        self.nodes[idx].key.clear();
        self.free.push(idx);
    }

    /// Вытесняет и возвращает ключ хвоста (least-recently-used), если есть. O(1).
    fn evict(&mut self) -> Option<Vec<u8>> {
        if self.tail == NIL {
            return None;
        }
        let idx = self.tail;
        let key = std::mem::take(&mut self.nodes[idx].key);
        self.unlink(idx);
        self.free.push(idx);
        Some(key)
    }

    /// Внутреннее: правит prev/next соседей, чтобы обойти `idx`, обновляет head/tail
    /// если `idx` был крайним. Не трогает собственные prev/next `idx` и не освобождает слот.
    fn unlink(&mut self, idx: usize) {
        let (prev, next) = (self.nodes[idx].prev, self.nodes[idx].next);
        match prev {
            NIL => self.head = next,
            p => self.nodes[p].next = next,
        }
        match next {
            NIL => self.tail = prev,
            n => self.nodes[n].prev = prev,
        }
    }
}
```

`Entry` дополняется приватным полем-индексом в арену:

```rust
#[derive(Debug, Clone)]
pub struct Entry {
    pub value: Vec<u8>,
    pub expires_at: Option<Instant>,
    lru_idx: usize, // индекс в Store::lru.nodes — приватное, не часть публичного API
}
```

`Store`:

```rust
#[derive(Default)]
pub struct Store {
    map: HashMap<Vec<u8>, Entry>,
    lru: LruList,
    capacity: Option<usize>, // None = без лимита (по умолчанию, совместимо с Этапами 2-3)
}

impl Store {
    pub fn new() -> Self { Self::default() }

    /// Store с ограниченной ёмкостью; при переполнении вытесняется наименее недавно
    /// использованный ключ перед вставкой нового.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { capacity: Some(capacity), ..Self::default() }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.is_expired(key) {
            self.remove_internal(key);
            return None;
        }
        let idx = self.map.get(key)?.lru_idx;
        self.lru.move_to_front(idx);
        self.map.get(key).map(|e| e.value.clone())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: Option<Instant>) {
        if let Some(existing) = self.map.get_mut(&key) {
            existing.value = value;
            existing.expires_at = expires_at;
            let idx = existing.lru_idx;
            self.lru.move_to_front(idx);
            return;
        }

        if let Some(cap) = self.capacity {
            if self.map.len() >= cap {
                self.evict_one();
            }
        }

        let idx = self.lru.push_front(key.clone());
        self.map.insert(key, Entry { value, expires_at, lru_idx: idx });
    }

    pub fn del(&mut self, key: &[u8]) -> bool {
        self.remove_internal(key)
    }

    fn remove_internal(&mut self, key: &[u8]) -> bool {
        match self.map.remove(key) {
            Some(entry) => { self.lru.remove(entry.lru_idx); true }
            None => false,
        }
    }

    fn evict_one(&mut self) {
        if let Some(key) = self.lru.evict() {
            self.map.remove(&key);
        }
    }

    // ttl_secs / expire / is_expired — без изменений по сигнатуре с Этапа 3.

    pub fn sweep_expired(&mut self) {
        let now = Instant::now();
        let expired_keys: Vec<Vec<u8>> = self.map.iter()
            .filter(|(_, e)| matches!(e.expires_at, Some(at) if at <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for k in expired_keys {
            self.remove_internal(&k);
        }
    }
}
```

Изменение `sweep_expired` относительно Этапа 3: раньше использовался `self.map.retain(...)`,
что было O(n) без побочных структур. Теперь, раз в `LruList` нужно ещё и `unlink` каждый
удаляемый узел, `retain` с доступом к `self.lru` внутри замыкания невозможен из-за заимствования
`self` целиком — поэтому ключи для удаления собираются в промежуточный `Vec`, затем удаляются
через `remove_internal`, которая согласованно чистит обе структуры. Асимптотика остаётся O(n)
по числу ключей за один sweep (sweep и не обязан быть быстрее линейного — он проходит весь стор
по построению).

`capacity: Option<usize>` — обратная совместимость: `Store::new()`/`Store::default()` остаются
безлимитными (используются во всех тестах Этапов 2–3 без изменений), `Store::with_capacity(n)`
— новый явный конструктор для ограниченного режима.

#### Конфигурация ёмкости в `main.rs`

```rust
fn main() -> std::io::Result<()> {
    // ...
    let capacity = std::env::var("MINIREDIS_CAPACITY").ok().and_then(|s| s.parse().ok());
    let store = Arc::new(Mutex::new(match capacity {
        Some(cap) => Store::with_capacity(cap),
        None => Store::new(),
    }));
    // ...
}
```

Переменная окружения `MINIREDIS_CAPACITY` — простейший способ параметризовать ёмкость без
парсера CLI-флагов. Способ конфигурации — открытый вопрос ergonomics (см. ниже), не влияет на
структуру `Store::with_capacity`.

#### Обработка ошибок

LRU-вытеснение не порождает новых клиентских ошибок протокола — вытеснение прозрачно (как
`maxmemory-policy allkeys-lru` в настоящем Redis): клиент получает обычный `+OK` на `SET`, даже
если это вызвало вытеснение другого ключа. Никакого предупреждения клиенту не посылается —
сознательное соответствие поведению Redis.

Внутренний инвариант (проверяется тестами, не клиентская «ошибка»):
`self.map.len() == self.lru.nodes.len() - self.lru.free.len()` в любой момент — арена и карта
синхронизированы. Полезен как debug-проверка в тестах, не обязателен в релизной сборке.

#### Тестовый план

Юнит-тесты `LruList` в изоляции (приватный тип модуля, тестируется напрямую): LIFO-порядок
вытеснения для одной цепочки; `move_to_front` переупорядочивает и меняет следующего кандидата
на вытеснение; `move_to_front` на уже-голове — no-op; удаление среднего узла корректно
перелинковывает соседей; переиспользование освобождённого слота из `free`; `evict` на пустом
списке → `None`.

Юнит-тесты `Store` с ограниченной ёмкостью: переполнение вытесняет LRU-ключ, не тронутый;
`get` считается touch и предотвращает вытеснение touched-ключа; повторный `set` существующего
ключа тоже считается touch; перезапись существующего ключа не увеличивает счётчик занятости
(вытеснения не происходит); `del` освобождает слот ёмкости; безлимитный `Store::new()` никогда
не вытесняет даже на существенно большом числе ключей (например 1000); истёкший (TTL) ключ при
capacity=1 вытесняется LRU-логикой при следующем `set`, а не TTL-логикой — `set` **не** делает
ленивую проверку истечения перед подсчётом занятости места (сознательное решение: TTL-очистка —
задача sweeper'а/ленивой проверки на `get`, не `set`); тест должен явно фиксировать это как
задокументированное поведение, не как побочный эффект; `sweep_expired` корректно отвязывает
узел от LRU-арены, а не только от `HashMap` (иначе `free`-список арены разойдётся с картой) —
проверяется многошаговой цепочкой `set`/`sweep_expired`/`set` с явными промежуточными assert'ами,
а не только финальным итогом.

Property/consistency тест: детерминированная последовательность из нескольких сотен операций
`set`/`get`/`del` на небольшом пространстве ключей (10 ключей, тот же ручной ГСЧ из Этапа 1) на
`Store::with_capacity(5)`; после каждой операции — инвариант `map.len() == живых узлов арены`
(тестовый геттер `#[cfg(test)] fn lru_len(&self) -> usize`, либо проход по списку от `head` до
`NIL`, что заодно ловит битые связи/циклы).

Критерий O(1) подтверждается не микробенчмарком с ассертом на наносекунды (нестабильно в CI), а
структурно: код `move_to_front`/`evict`/`push_front`/`remove` не содержит циклов по всей
коллекции — только точечный доступ по индексу/ключу и переключение нескольких `prev`/`next`.
Опциональный `#[ignore]`-бенч (`cargo test -- --ignored`) может дать эмпирическое число для
отчёта, но без жёсткого ассерта на конкретные наносекунды.

#### Критерий готовности

- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` чисты.
- Ревью кода подтверждает отсутствие сканирования по `nodes`/`map` в горячих методах LRU.
- Живой `redis-cli`:
  ```
  MINIREDIS_CAPACITY=2 cargo run &
  redis-cli -p 6379 set a 1
  redis-cli -p 6379 set b 2
  redis-cli -p 6379 get a          # touch a -> a становится MRU
  redis-cli -p 6379 set c 3        # переполнение -> вытесняется b (LRU), не a
  redis-cli -p 6379 get b          # (nil) -- вытеснен
  redis-cli -p 6379 get a          # "1"  -- жив, был touched
  redis-cli -p 6379 get c          # "3"  -- жив, только что вставлен
  ```

#### Открытые решения

- **Slab-арена vs крейт `lru`.** План специфицирует slab-путь как реализацию по умолчанию. При
  непропорциональной сложности/цене переход на `lru` остаётся допустимым запасным путём, но
  требует явной записи причины в этом файле (данный раздел) до мержа.
- **Способ конфигурации `capacity`** (переменная окружения vs второй CLI-аргумент vs константа)
  — предложен один вариант выше для конкретности, не зафиксирован как обязательный.

### Этап 5 — Конкурентная модель + нагрузочный тест

Этот Этап не добавляет новых структур данных, а закаляет написанное в Этапах 2–4 и проводит
нагрузочную проверку. Переписывать архитектуру не требуется.

#### Файлы и изменения

**Mutex poisoning.** Код использует `store.lock().unwrap()` в нескольких местах `command.rs`.
Если поток запаникует, удерживая блокировку (не должно происходить намеренно, но `unwrap()` на
`PoisonError` уронил бы каскадно все остальные потоки — хуже, чем уронить только паникующий).
Решение — хелпер, который восстанавливается после отравления, не паникует повторно:

```rust
fn lock_store(store: &Arc<Mutex<Store>>) -> std::sync::MutexGuard<'_, Store> {
    store.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
```

Заменить все `store.lock().unwrap()` в `command.rs` на `lock_store(store)`. Обоснование: паники
на пользовательский ввод исключены дизайном (SKILL.md), poisoning — защита от гипотетического
бага, а не ожидаемый путь; Rust гарантирует отсутствие data race даже после poisoning, только не
логическую консистентность простых инвариантов проекта.

**`handle_connection` — устойчивость:** `stream.read()`/`stream.write_all()` ошибки уже
обрабатываются через `return` без `unwrap()` (Этап 2) — подтвердить тестом, что обрыв
соединения посреди фрейма не роняет сервер. Опционально — логирование на `stderr` при
неожиданном закрытии, не обязательное требование.

**Модель блокировки не меняется.** Этап 5 сознательно не переходит на шардирование/`RwLock`
даже если бенчмарк покажет мьютекс узким местом — задача Этапа: измерить и задокументировать
наблюдение в `POST_MVP_PLAN.md`, пункт 7, не оптимизировать.

**Небольшой структурный рефакторинг — `src/lib.rs`.** До этого Этапа ни один тест не нуждался в
настоящем TCP-сокете, поэтому библиотечный корень не заводился. Для e2e-теста с реальными
сокетами внутри `cargo test` заводится `src/lib.rs`:

```rust
pub mod command;
pub mod resp;
pub mod store;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use store::Store;

pub fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    // тело переносится из src/main.rs без изменений
}

pub fn spawn_sweeper(store: Arc<Mutex<Store>>) {
    // тело переносится из src/main.rs без изменений
}
```

`src/main.rs` становится тонким бинарным враппером (`use miniredis::{handle_connection,
spawn_sweeper, store::Store};`, `mod`-объявления убираются): `main()` парсит адрес/ёмкость,
биндит `TcpListener`, крутит цикл `accept` + `thread::spawn(handle_connection)`. Это
единственное структурное изменение в компоновке файлов за все пять Этапов — отметить явно в PR.

#### Обработка ошибок

Ничего принципиально нового сверх Этапов 2–4; фокус — устранение путей паники под конкурентной
нагрузкой. Одновременный `SET`/`GET`/`DEL` на один ключ с разных соединений — корректность
гарантирована `Mutex` (сериализация доступа). Резкий разрыв TCP-соединения посреди отправки
кадра не должен ронять другие соединения или процесс.

#### Тестовый план

Конкурентность без сети (`tests/concurrency.rs` или в `src/store.rs`): 8 потоков делают 1000
циклов `set`/`get` каждый на своих ключах — без паник, финальные значения корректны; 8 потоков
пишут в один и тот же ключ — итоговое значение равно ровно одному из записанных (`Mutex` не даёт
«порвать» `Vec<u8>` посередине); юнит-тест на `lock_store` — намеренно отравить `Mutex` (поток
паникует, удерживая лок), убедиться, что хелпер возвращает `MutexGuard`, а не паникует повторно.

Интеграционный тест с реальными сокетами (`tests/network_e2e.rs`, единственный во всём наборе,
что поднимает настоящий listener, а не только библиотечные вызовы): `TcpListener::bind
("127.0.0.1:0")` (порт 0 — ОС выбирает свободный) + `local_addr()`; PING/PONG по настоящему
сокету; несколько склеенных кадров в одном `write_all` — распарсить обратно на стороне теста и
проверить каждый по отдельности (бьёт по «несколько кадров в одном чанке» уже на настоящем
сокете); отправить половину кадра и оборвать соединение — новое соединение после этого всё ещё
отвечает на PING (сервер не упал и не завис); 20 параллельных клиентов, каждый SET+GET на свой
уникальный ключ.

Ручной нагрузочный прогон (часть критерия готовности, не автоматизированный тест, результат
фиксируется в PR-описании):

```bash
cargo build --release
./target/release/miniredis &
redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -q
```

Ожидается: без ошибок соединения/таймаутов, throughput выведен для `SET` и `GET`. Дополнительно
прогнать `-c 200` один раз, чтобы увидеть поведение под большим числом потоков-обработчиков —
ожидаемо и приемлемо для профиля pet-проекта (десятки-сотни соединений, не 10k+).

#### Критерий готовности

- `cargo test` (включая `tests/concurrency.rs` и `tests/network_e2e.rs`) зелёный при нескольких
  повторных запусках подряд без флапа. `cargo clippy -- -D warnings`, `cargo fmt --check` чисты.
- `redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -q` проходит без ошибок, throughput
  зафиксирован в PR-описании.
- Обрыв соединения посреди фрейма не роняет сервер и не мешает новым подключениям (тест или
  ручная проверка).
- В PR/commit явно зафиксировано, что решение о шардировании/`RwLock` сознательно отложено (и,
  если мьютекс оказался узким местом при `-c 200`, — короткая заметка в `POST_MVP_PLAN.md`,
  пункт 7).

#### Открытые решения

Шардирование/`RwLock` — преднамеренно остаётся в POST_MVP независимо от результатов бенчмарка;
Этап 5 обязан зафиксировать наблюдение, не действовать на его основе. Graceful shutdown
(`SIGINT`/`SIGTERM`, join фонового sweeper-потока) не требуется MVP ни на одном из Этапов 1–5.
Расположение общего хелпера для `real_tcp_*`-тестов (`tests/common/mod.rs` vs дублирование) — на
усмотрение реализатора.

## Тестовая стратегия (сквозная)

Соответствует конвенции портфеля «тестам нужен интеграционный ярус»:

- **Юнит:** RESP-кодек (Этап 1) — включая фрагментацию; логика TTL и LRU в изоляции от сети.
- **Интеграция (детерминированный фейк, не только мок-HTTP):** поднять `Store` в памяти,
  прогнать реальные последовательности команд через настоящий кодек, без сети — детерминированно
  и быстро в CI.
- **End-to-end (ручной/по требованию, плюс автоматизированный сокетный слой с Этапа 5):**
  настоящий `redis-cli` и `redis-benchmark` против запущенного бинаря — то, что мок никогда не
  поймает (реальная фрагментация TCP, поведение реального клиента); с Этапа 5 дополнено
  автоматизированными тестами на реальных `TcpStream` в `tests/network_e2e.rs`.

Перед PR каждого Этапа: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`, плюс
минимум одна ручная проверка `redis-cli`/`redis-benchmark`, специфичная для Этапа (см.
«Критерий готовности» выше).
