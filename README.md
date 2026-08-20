# miniredis-rs

Учебный in-memory key/value сервер на Rust, который **сам реализует проволочный протокол
Redis (RESP)** поверх голого TCP — не HTTP-обёртка, а настоящий бинарный протокол. С TTL и
LRU-вытеснением за O(1).

Проект про технику, не про продукт. Главные темы: потоковый разбор бинарного протокола
(корректная обработка **частичных TCP-чтений**), разделяемые структуры данных под конкурентный
доступ, eviction-алгоритм LRU за O(1).

## Статус

MVP готов: Этапы 0–5 реализованы, смержены и покрыты тестами (RESP2-кодек, команды,
TTL, LRU-вытеснение O(1), конкурентная модель — см. [`docs/PLAN.md`](docs/PLAN.md#этапы)).
Дальнейшая работа — по [`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md).

## Быстрый старт

```bash
cargo run                 # слушает 127.0.0.1:6379 (как настоящий Redis)
cargo run 127.0.0.1:7000  # другой адрес/порт — первым аргументом
MINIREDIS_CAPACITY=1000 cargo run  # ограничить LRU-ёмкость (по умолчанию безлимитно)
```

## Тестирование настоящим redis-cli

Сервер говорит на RESP, поэтому его можно гонять штатным клиентом Redis — никакого
специального клиента писать не нужно:

```bash
redis-cli -p 6379 ping
redis-cli -p 6379 set foo bar ex 60
redis-cli -p 6379 get foo
redis-cli -p 6379 ttl foo
redis-cli -p 6379 expire foo 120
redis-cli -p 6379 del foo
```

Нагрузочная проверка — тоже штатным инструментом:

```bash
redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -q
```

## Документация

- [`docs/PLAN.md`](docs/PLAN.md) — видение, архитектура, Этапы, «После MVP».
- [`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md) — стек, ключевые решения, детальная
  разбивка по Этапам.
- [`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md) — pub/sub, транзакции, персистентность,
  репликация и прочее вне MVP.

## Скоуп MVP

In-memory KV + TTL + LRU. Команды: `PING`, `GET`, `SET` (с `EX`/`PX`), `DEL`, `EXPIRE`, `TTL`.
Вырезано из v1 полностью: pub/sub, транзакции (MULTI/EXEC), персистентность (RDB/AOF),
репликация.

## Лицензия

MIT — см. [`LICENSE`](LICENSE).
