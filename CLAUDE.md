# CLAUDE.md

Guidance for Claude Code when working in the `miniredis-rs` repository.

## Что это

Учебный in-memory KV-сервер на Rust со своей реализацией протокола RESP (Redis wire protocol)
поверх TCP, с TTL и O(1) LRU-вытеснением. Pet-проект соло-разработчика: цель — техническая
сложность и обучение, не бизнес. Бюджет $0, внешних зависимостей и Docker нет —
самодостаточный бинарь.

Документация: [`docs/PLAN.md`](docs/PLAN.md), [`docs/TECHNICAL_PLAN.md`](docs/TECHNICAL_PLAN.md),
[`docs/POST_MVP_PLAN.md`](docs/POST_MVP_PLAN.md). Конвенции разработки —
[`.claude/skills/rust-miniredis-dev/SKILL.md`](.claude/skills/rust-miniredis-dev/SKILL.md).

## Раскладка

| Путь | Содержимое |
|---|---|
| `src/main.rs` | точка входа: TCP-listener, thread-per-connection, диспатч |
| `src/resp.rs` | протокольный слой: буферизующий возобновляемый RESP-кодек |
| `src/store.rs` | слой данных: KV-хранилище, TTL, LRU |
| `docs/` | PLAN, TECHNICAL_PLAN, POST_MVP_PLAN |

## Ключевые технические решения (зафиксированы)

- **Только `std::net`, без `tokio` в v1.** thread-per-connection. Async спрятал бы главную
  учебную мину — потоковый парсинг с фрагментацией. Миграция на tokio — POST_MVP.
- **RESP-парсер обязан переживать фрагментацию TCP-чтений.** Буферизующий, возобновляемый
  декодер; `try_parse()` возвращает `Incomplete` на границе кадра, не потребляя частичные байты.
- **LRU за O(1)** — свой intrusive linked list (безопасный вариант: slab/арена с
  индексами вместо ссылок). Крейт `lru` — запасной путь, выбор фиксировать в TECHNICAL_PLAN.

## Команды

```bash
cargo build                 # сборка
cargo check                 # быстрая проверка типов
cargo run                   # запуск сервера на 127.0.0.1:6379
cargo test                  # все тесты (юнит + интеграционные фейки, без сети)
cargo fmt                   # форматирование
cargo fmt --check           # проверка форматирования
cargo clippy -- -D warnings # линт, warnings как ошибки
```

Перед коммитом прогнать: `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo test`.
End-to-end (по требованию): `redis-cli -p 6379 ...`, `redis-benchmark -p 6379 ...`.

## Конвенции проекта

- **Язык:** документация и subject коммитов — на русском; код, идентификаторы и
  комментарии в коде — на английском.
- **Коммиты:** conventional-commit с русским subject, напр.
  `feat(resp): буферизующий парсер RESP с обработкой фрагментации`. Завершать коммит трейлером
  `Co-Authored-By: Claude`.
- **Ветки:** новая ветка от `master` на каждый Этап (напр. `stage/1-resp-codec`) → PR → merge
  в `master`. PR target — `master`.
- Заглушки помечать `// TODO(Этап N): ...`, снимать по мере реализации.

## Пайплайн разработки (по Этапам)

Актуальный проверенный пайплайн портфеля. Для каждого Этапа:

1. **Планирование + написание кода — Opus 4.8.** Спланировать Этап (по TECHNICAL_PLAN),
   реализовать код.
2. **Проверка покрытия + тестирование + работоспособность — Sonnet 5 (основной чат).**
   Проверить тестовое покрытие, дописать тесты (обязательно — фрагментационные тесты парсера,
   см. SKILL.md), проверить, что реально работает (в т.ч. живой `redis-cli`).
3. **Независимое ревью — Opus через Agent-тул (`model: opus`), `/code-review` на diff ветки.**
   Ревью запускать на diff ветки Этапа против `master`.
4. **Цикл исправлений — до 3 итераций.** Правки по замечаниям ревью, повторное ревью; не более
   трёх кругов.
5. **Commit + push + PR в `master`.** Conventional-commit, русский subject, трейлер
   `Co-Authored-By: Claude`. Мержить в `master`.

Не коммитить и не пушить без явной просьбы пользователя.
