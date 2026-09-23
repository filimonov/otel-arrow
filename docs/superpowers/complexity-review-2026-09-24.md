# Complexity review (user-supplied, 2026-09-24)

The text below was pasted by the user in Russian. Triage against the code after Task 3f and slices S1-S4 is at the end.

Где сложность лишняя, по убыванию веса

1. Классификация исходов запроса размазана на четыре слоя. lake::RefuseReason → Failure {Permanent, Retryable} → Outcome (11 вариантов) → NackCause / NackErrorType. Два классификатора одной ошибки (Failure::classify и reservation_failure), Prepared как переизобретённый Result, ручной счётчик OUTCOMES, таблица reason(), таблица маппинга. При этом seal- и admit-ошибки всё равно помечаются как Storage. Достаточно одного enum с AttributeEnum и одной функции «ошибка → (outcome, permanent)».
2. Состояние worker'а живёт в двух файлах. drive в mod.rs это select на 180 строк, который мутирует семь pub(super) полей Worker'а и повторяет guard, который rotate() проверяет сам. Инварианты «один ACTIVE, один FLUSHING, один parked» доказываются частично guard'ами select'а, частично методами. Именно на этом шве сидит найденный баг с notifier-кредитом: force_shutdown не видит токены, которые держат блоки. Если каждая ветка select'а станет одним методом Worker'а, drive сжимается до ~60 строк, а кредит считается в одном месте.
3. Конфиг существует в двух формах. Exporter хранит window.* и копирует часть в lake.ingress.*, worker читает лимит блока то из одного, то из другого. Lake жёстко называет свои ключи в сообщениях, поэтому exporter повторяет четыре правила валидации со своими ключами. Ошибка конфига едет как request-refusal и распаковывается двумя match'ами. Одна форма секций плюс Error::Config убирает mirror-структуры, дубли проверок и распаковку.
4. Стек записи в sink. AsyncArrowWriter → LedgeredWriter → ParquetObjectWriter → BufWriter → CreationWatch → LedgeredUpload → store, 13 вспомогательных типов ради одной таблицы. Из них лишние: ParquetObjectWriter (адаптер из двух методов, который приходится разворачивать обратно для abort), PutLanded (это PartLanded со span от нуля), две пары gauge+high-water+guard с одинаковым кодом. Сама write_table на 221 строку с ручным failure = Some(..); break десять раз. Это не архитектурная сложность, а отсутствие ?.
5. Промежуточное представление строк в extraction. Col (11 вариантов) → ValuesRow → AnyBuilder (ещё 11 вариантов) → рантайм-проверка ширины → Error::internal. Три датасета имеют схемы, известные при компиляции. Слой добавляет аллокацию на ячейку и ловит только собственные баги крейта. Плюс два разных memo для одной задачи и 22 хелпера чтения ячеек. Это ещё и главный источник CPU-накладных в extraction.
6. Merge step machinery. StepBudget, MergeBuild, MergeIter, ChunkBuilder, MergeStep::Ready/Chunk, протокол chunk_builder → step → finish → chunk_taken, который каждый вызывающий пишет заново, Result на шагах «которые сегодня не падают», test-only поле last_step_rows. Бюджет по строкам за шаг нужен, остальное общность на один вызов.
7. Engine residual. 600 строк в generic engine-крейте, глобальный mutex, передача владения репортером, test hook в production-структуре, всё ради числа max(0, RSS − Σ accounted), которое бэкенд посчитает из двух уже существующих gauge. Это чистое накопление ради измерительной кампании.
8. Python-харнесс как второй продукт. 29k строк, библиотека размазана по CLI-модулю и тест-модулю, шесть копий хореографии engine-lifetime, четыре конвенции subprocess, 47 копий одного тернарника. Здесь сложность не в задаче, а в том, что каждая новая семья измерений копировала предыдущую вместо импорта.

Что я бы упростил первым

- Пункты 1 и 2 вместе. Они на одном шве, и там же живёт единственный подтверждённый панический баг. Один enum исходов, Worker владеет своим состоянием, drive только выбирает future и вызывает метод.
- Пункт 3. Одна форма конфига убирает дубли проверок и целый класс несогласованности «читаем лимит из разного места».
- Пункт 4 частично: write_table через ?, минус слой ParquetObjectWriter и PutLanded. Ledger и watch оставить.
- Пункт 7 вынести из ветки целиком.

Пункты 5 и 6 я бы делал вторым заходом. Они дают выигрыш по производительности и объёму кода, но требуют аккуратности и покрыты oracle- и fuzz-тестами, которые нужно держать зелёными.

## Triage (controller, 2026-09-24; user decision: defer everything that can wait to plan 4)

- 1, outcome classification: done in Task 3f (Failure, NackErrorType, Prepared and the second classifier are gone; one Outcome; OUTCOMES checked at compile time). Seal/admit failures labelled Storage are fixed in slice S7. `derive(AttributeEnum)` for Outcome: plan 4.
- 2, worker state across mod.rs and worker.rs (56 `pub(super)` fields, `drive` mutating them, the rotation guard checked twice): plan 4, with the worker state machine and the shared writer. Slice S7 fixes blocker 1 locally by passing the tokens held outside the notifier into `force_shutdown`.
- 3, config in two forms (exporter mirror structs Window/Ingress/Parquet, lake messages naming lake keys, config errors carried as request refusals; deslop B9): plan 4.
- 4, sink write stack: `write_table` with `?` and one checkpoint done in slice S4; merging `PutLanded` into `PartLanded` and one gauge type for the two gauge/high-water/guard pairs: plan 4.
- 5, extraction intermediate representation: slice S6 (typed builders, one memo, no body copies), each step with a stage measurement.
- 6, merge step machinery: the reasonable part done in slice S4 (infallible step, test-only counters, one drain helper); the rest is what bounds a step (Task 3i) and stays.
- 7, engine residual: removed at the history rewrite before the upstream PR (already decided).
- 8, Python harness: decided at upstream time (already decided).
