# План де-слопа: series-parquet-exporter (5588c3e0d..HEAD)

Шесть параллельных проходов по коду ветки: экстракция, buffer/sort/sink, экспортер и
engine-склейка, валидатор и Rust-тесты, Python-харнесс, отдельный проход по комментариям
и прозе. Отчёты по сиденьям: `/tmp/claude-1000/.../scratchpad/deslop{1..6}-*.md`.
Сводка ниже, отсортирована по payoff / effort. Принципы: KISS, DRY, минимальный SOLID
(только там, где он убирает код, а не добавляет слой).

## Цифры

| Что | Где | Сколько |
|---|---|---|
| Комментарии в production-коде | window.rs / flush.rs / worker.rs / error.rs | 48% / 39% / 32% / 41% |
| Блоки комментариев 6+ строк | все Rust-файлы ветки | 285 блоков, 2596 строк; оставить стоит 88 блоков (796 строк) |
| Преамбулы `Scenario:/Guarantees:` | Rust + Python | 629 в текущем worktree (773 в diff), ~1470 строк только в Rust |
| «rather than» в комментариях | экспортер + lake | 156 |
| « -- » вставки-отступления | Rust-комментарии | 147 |
| Ссылки «spec section N» / «plan 3» / история фиксов | 14 файлов | 38 + ~6 строк |
| Делегирующие обёртки `ObjectStore` (5 одинаковых pass-through методов) | sink.rs, tests.rs, parquet_exporter | 8 копий (~300 строк) |
| Хелперы чтения ячеек Arrow | extract/, attrs.rs | 22 именованных + 7 inline-копий, одна уже разошлась (`metric_type` без `is_valid`) |
| Формулы размера | extract/, config.rs, buffer.rs | 11 функций/констант, две пары дублируют друг друга |
| Функции > 100 строк (Python) | harness | 31, максимум `exercise_outage` 516 |
| Тройной импорт `try: from . import x / except ImportError` | harness | 25 мест, прячут циклы |
| Тернарник `PASSED if ok else FAILED` | harness | 47 копий |
| `_ = call()` без линтера | harness | 285 строк |
| Мёртвый код | разное | `Error::Pdata`, `AttrTable::approx_bytes`, `FaultStore mode == 4`, `ProcessorInbox::shutdown_deadline`, 6 stub-подкоманд CLI |
| Реальный баг, найденный попутно | measure.py:953 | `LEGACY_TEST_COUNT = 18`, тестов 19: hard-check `legacy_tests_passed` падает на чистом прогоне |

## Часть A. Комментарии и проза (наибольший payoff, почти нулевой риск)

A1. **Удалить преамбулы `Scenario:/Guarantees:` везде** (Rust и Python). Имя теста уже
    60-68 символов и говорит то же самое. Оставить 1-2 строки только там, где setup
    неочевиден (платформенный трюк, намеренно невалидный конфиг, пин конкретного бага);
    таких ~20 из 629. Запретить `/// Scenario:` грепом в CI. Минус ~2000 строк.

A2. **Одно объяснение на концепт.** Шесть механизмов объяснены по 3-5 раз каждый:
    - «deadline-ветка выше flush-result, поэтому завершившуюся запись всё равно ack-аем»:
      flush.rs:31-40, 163-166, 440-448; worker.rs:1370-1402; tests.rs:5417. Дом: `FlushJob::try_finish`.
    - «byte views ленивые, порча даёт ack нуля строк»: worker.rs:13-33, 626-640; tests.rs:1063; README:1252. Дом: validate.rs module doc.
    - «нормальные completions останавливаются за один слот до capacity»: token.rs:364-385, 498-510; mod.rs:268-281. Дом: `Notifier::has_credit`.
    - формула worst-case резерва: buffer.rs:1108-1120, config.rs:451-461, README:140-161. Дом: `series_row_fixed_bytes`.
    - «cast словаря разворачивает его до бюджета»: attrs.rs:43-55, 247-261; README:1257.
    - «concat скопировал бы все строки зря»: buffer.rs:79-91, 199-210.
    Остальные копии заменить на `[ссылку]`.

A3. **Сократить четыре module doc, пересказывающие одну state-machine**: mod.rs:4-30,
    worker.rs:4-48, flush.rs:4-40, window.rs:4-35 (141 строка). По 1-3 предложения на модуль.
    В window.rs оставить единственное реальное внешнее ограничение: «engine отменяет
    periodic timers до drain'а receivers, поэтому sleep свой». Остальные худшие блоки с
    готовой заменой в одно предложение: deslop6-prose.md, часть 1 (15 штук).

A4. **Убрать историю и ссылки на процесс из кода**: «spec section N» (38 строк),
    «plan 3's work», «In the fixed code…», «Before the memo was keyed…», «the previous
    review». Инвариант оставить, цитату удалить или заменить ссылкой на FORMAT.md.

A5. **Фразовый свип**: удалить хвост «rather than <очевидная альтернатива>», переписать
    клефты «is what keeps» в прямую причину, разбить или удалить « -- » вставки.
    Три грепа дают ~450 мест.

A6. **Исправить прозу, противоречащую коду** (сверх того, что уже в umbrella-отчёте):
    README:65-68 (запись, завершившаяся к deadline, ack-ается, а не nack-ается);
    README:209-246 (заголовок называет `window.interval` главным рычагом, свои же цифры
    показывают, что это `flush_retry_deadline`); README:1191 («nothing else is lost»);
    README:1193 («refused = unsupported», а типов 10); worker.rs:27-28 и README:1255
    (UTF-8 проверяется).

A7. **README экспортера 1391 -> ~430 строк.** Сначала удалить тест
    `readme_states_operating_contract` (tests.rs:5680-5713), который грепает README на 18
    фраз и весь Alloy-файл verbatim и блокирует любое сокращение. Затем: Examples (355-776,
    421 строка: inline Alloy, 150-строчный Python-скрипт, image env vars) уезжает в
    validation/tests/series_parquet/README.md с 6-строчным указателем; durable_buffer
    (778-880) сокращается до 8 строк + ссылка; лимиты дедуплицируются с series-lake README.
    Постатейный план: deslop6-prose.md, часть 3.

## Часть B. Интерфейсы и дублирование в Rust (средний effort, низкий риск)

B1. **Один хелпер чтения ячейки вместо 22 + 7.** `fn prim_at<T: ArrowPrimitiveType>(a, row)
    -> Option<T::Native>` и `fn typed_col<T>(batch, name, what) -> Result<Option<&T>>`.
    Закрывает реальный дрейф: metrics.rs:193 читает `metric_type` без `is_valid`.
    Заодно: `flat_child` (flatten всей структуры ради одного child) слить в `flat_children`.

B2. **Одна формула размера.** Удалить `AttrTable::approx_bytes` (без вызовов), свести
    `map_cell` и `rendered_kv_bytes` к одному `entry_bytes`, вывести число колонок series
    из `dataset_schema(...).fields().len()` вместо «10 + 6» в config.rs:466, extract/mod.rs:79
    и buffer.rs test. Константы 24/64/8 назвать. Удалить тест, который существует только
    чтобы две копии формулы совпадали.

B3. **Один memo для logs и metrics.** Metrics уже на borrowed content-key; logs клонирует
    все атрибуты, аллоцирует 4 String и canonical-кодирует фиктивный Descriptor на каждой
    строке до lookup'а. Вынести `kv_eq`/`hash_value` в extract/mod.rs, ключ logs сделать
    borrowed, `Descriptor` строить только на miss. Это же закрывает perf-finding из
    umbrella-обзора.

B4. **Слить два цикла точек в `extract_metrics`** (248 строк, ~35 общих строк на цикл):
    один `push_point` с closure на kind-специфичные 6 ячеек; `series_for` возвращает
    `(SeriesId, &MetricRow)`, чтобы не делать `metric_of` второй раз. Поднять
    `denorm_columns` из per-point цикла, как уже сделано в logs.

B5. **`Block<T>` -> `Block`.** Production использует только `Block<()>` и держит токены
    рядом; `requests: Vec<T>` это счётчик. Убрать T из `Sink::write_block`, `planned_paths`,
    bench-хелперов. `into_parts` только для тестов: `#[cfg(test)]` или удалить.
    `Block::reserve` принимает `cfg`, который блок уже владеет, и bool `reemit` — убрать оба.

B6. **`write_table` 221 строка -> три функции.** 5 копий «yield + is_cancelled + break»,
    10 `failure = Some(..); break`. Вынести `checkpoint()` и `write_chunks()`, вернуть `?`.
    Убрать слой `ParquetObjectWriter` (LedgeredWriter реализует `AsyncFileWriter` прямо
    над `BufWriter`), слить `PutLanded` в `PartLanded` (span от нуля), слить `MergeKeys`/
    `FlushWorkspace` в один `Gauge { current, high }` с drop-guard. `CreationWatch` и
    ledger оставить: они закрывают реальные гонки.

B7. **Один `Clock` trait вместо трёх стилей** (`WallClock` trait, `SinkClock` из fn-pointer'ов
    с ручным Debug и boxed alias, engine free-функции). `Sink` и `window` получают один
    объект. `TestWallClock` (публичный, без cfg, 67 использований из core-nodes) за
    feature `testing`.

B8. **Merge step API**: `step()` возвращает `Result`, документированный как «Never, today»;
    `last_step_rows` production-поле, которое читает один тест; цикл «Ready -> chunk_builder
    -> finish -> chunk_taken» переписан 3 раза в production и 9 раз в тестах. Сделать
    инфаллибельным, `ChunkBuilder::step -> Option<RecordBatch>`, `drain()` в тестах.

B9. **Config: одна форма, одна проверка.** Сейчас лимиты блока хранятся дважды
    (`window.*` и `lake.ingress.*`) и читаются из разных мест в worker.rs:780/1127;
    exporter config.rs повторяет 4 правила `LakeConfig::validate` слово в слово, потому
    что lake в сообщениях жёстко называет свои ключи; ошибки конфига едут как
    `Error::Refused(RefuseReason::Invalid)` и распаковываются двумя match'ами. Решение:
    `Error::Config(String)` в lake, секции lake = пользовательские секции (перенести
    `max_block_bytes`/`max_requests_per_block`/`window_interval` в `window` внутри
    LakeConfig), удалить mirror-структуры `Ingress`/`Parquet`/`Window` и их Default'ы,
    удалить 4 дубля проверок. `SortKey`/`Denormalize`: `#[serde(from = "…Repr")]` вместо
    ручного Deserialize с identity-веткой; `SortKey::asc(col)` вместо литерала, написанного 20 раз.

B10. **Outcome и его четыре зеркала.** `Outcome` (11 вариантов) + `OUTCOMES = 11` +
     таблица `reason()` + `NackErrorType` (те же имена минус Ack) + 10-строчная таблица
     маппинга в `sample_metrics`. Заменить на `#[derive(AttributeEnum)]`, `Outcome::ALL`,
     `[u64; Outcome::ALL.len()]`, удалить `NackErrorType` и `OUTCOMES`. Пропуск варианта
     тогда не компилируется, вместо out-of-bounds в рантайме.

B11. **`validate_framing` один раз, на `OtapPayload`.** Шесть одинаковых блоков в четырёх
     экспортерах, каждый с 3-5-строчным комментарием «views ленивые». Один
     `OtapPayload::validate_otlp_framing() -> Result<()>`, одна причина nack
     (`NackCause::Refused`), одно имя события. У series-worker убрать signal-match, который
     только префиксует «logs»/«metrics» к ошибке, уже называющей сообщение.

B12. **`drive` (180 строк, select! с 8 ветками) не должен править `pub(super)` поля
     Worker'а.** Семь полей мутируются из mod.rs; guard ротации дублирует проверку внутри
     `rotate()`; `rotate(); resume_pending();` написано трижды. Дать Worker методы
     `rotation_ready()`, `cleaned(result)`, `rotate_and_resume()`, `refuse_forced(data)`,
     и futures `window_sleep()/flush_result()/cleanup_done()`, возвращающие `pending()` при
     пустом слоте. `drive` станет ~60 строк; поля станут приватными. Читать
     `inbox.shutdown_deadline()` один раз на сообщение, а не в PData-ветке и в хвосте.
     Слить `run`/`run_announced`/`drive` в одну точку входа, удалить `Startup`.

B13. **Мелочи в экспортере, каждая S:** `storage_kind` парсит Debug-вывод → `StorageType::kind()`;
     `Failure::excess` возвращает 3-tuple, разбираемый тремя closure → `Option<&Excess>`;
     `Prepared` это `Result<Pending, (AckToken, Failure)>` с абзацем, объясняющим, почему не Result;
     `write_until`: 4 копии `result_tx.send(FlushDone{..})` и два одинаковых `otel_info!/otel_debug!`;
     token.rs: 3 копии «poll once unconstrained, count failure»; `Metrics.exports` два мёртвых
     пути отчёта; `ExemplarAttrs { signal }` метка с одним возможным значением (осторожно:
     меняет метрику); `check_retry_deadline(retried: bool, …)` → принимать `&StorageType`;
     `SIGNAL_SHUTDOWN_GRACE = 60s` ручная копия константы контроллера.

B14. **Engine: `Series*` типы и `exporter.series_parquet` metric set в generic-крейте.**
     Переименовать в нейтральный `AccountedMemory`, убрать `_bytes` из имени метрики
     (свой же тест экспортера это запрещает), удалить `series_entity` (всегда = ключ
     монитора), `on_materialize` test-seam в production-структуре, `isolated()`
     (= `Arc::default()`), `bytes()` (читают только тесты). Удалить
     `ProcessorInbox::shutdown_deadline` и его 30-строчный тест (вызовов нет).
     main.rs: 17-строчный doc с benchmark-цифрами → 2 предложения; `println!` дублирует
     `system_info`; cfg-блок из 5 строк написан дважды → один `mod jemalloc_conf`.

B15. **Test-seams в production-типах**: `Worker::samples` (doc говорит «exists so a
     regression test can assert»), `store_override`, `task_finished`, `last_step_rows`,
     high-water getters без production-вызовов. Правило: строить объект под тестом напрямую
     и ассертить по эмитированным метрикам; либо `#[cfg(test)]` с однострочным doc.

B16. **pdata validate.rs: схема написана дважды** — `Message::singular` (голые номера
     полей) и `Message::field` (константы + wire types), ~350 строк, ничего не проверяет их
     согласованность. Один match с `Field { kind, singular: Option<&str> }`. Static-таблица
     хуже: теряет exhaustiveness. Заодно decode.rs: `field_value_range`/`field_range`/
     `value_range` различаются только Option/Result; старый `validate_message_wire_format`
     переизобретает `read_key`+`value_range`. Строковые `problem: &'static str` («wrong wire
     type for a known field» 6 раз) → enum с Display.

## Часть C. Тесты (Rust)

C1. **tests.rs 6104 строк -> 9 файлов + `support.rs`** (диапазоны строк в deslop4-tests.md,
    item 1). Setup-квадруплет (InMemory store, effects(), TestWallClock, Worker::new)
    повторён 59 раз → `Harness::new(cfg)`; inline nack-match 21 раз → `expect_nack(rx)`;
    `inbox()` хелпер существует, но 6 inline-копий конструируют канал вручную.

C2. **Одна `HookStore` вместо 8 ObjectStore-обёрток** (sink.rs ×4 test + CreationWatch,
    tests.rs GatedStore/FaultStore, parquet_exporter FailPutForPrefixStore). Trait
    `StoreHooks { before_put, before_multipart, wrap_upload }` с default-реализациями,
    делегирование написано один раз. Минус ~300 строк. `FaultStore`: u8-режимы →
    `enum Fault`, удалить мёртвый `mode == 4`, `Ordering::SeqCst` полным путём 46 раз → один import.

C3. **150-строчный ручной `tracing::Subscriber`** (12 no-op методов) → `Layer::on_event`
    из `tracing-subscriber`, который уже в workspace и уже используется в telemetry и otap.

C4. **Одно свойство тестируется на трёх слоях одинаковыми байтами**: `[0x0a, 0x01, 0x0a]`
    в validate.rs, pdata/otlp/tests.rs и exporter tests.rs. Экспортер оставляет один
    табличный тест «framing error → permanent Refused nack, блок не тронут»; остальные 5
    тестов и дубль в otlp/tests.rs удалить (~350 строк).

C5. **Тесты, которые не могут упасть или тестируют фикстуру**:
    `fuzz_canonical::encoding_is_order_independent` тавтологичен (обе стороны сортируются
    одним `sort_by`, `canonical_bytes` не сортирует; prop_assert'ы повторяют prop_filter'ы);
    два golden-теста сравнивают поля JSON с полями того же JSON; `assert_eq!(vectors.len(), 34)`
    и `(118, 26)` — change-detectors; `a_known_field_with_the_wrong_wire_type_is_refused`
    принимает любую ошибку. Удалить или привязать к коду под тестом. Ассерты на подстроки
    ошибок (19 в tests.rs) → match по варианту, где вариант есть.

C6. **Near-duplicate пары** (similarity 0.75-0.89): config-refusal тесты, которые уже есть
    строками в таблице `startup_rejects_invalid_configuration`; `the_factory_creates_*`
    ×2; refusal-atomicity ×5 с одной изменённой ручкой → таблица. Oracle: пары
    `*_descriptor`/`*_expected` с 7 позиционными параметрами → строить Descriptor один раз
    и выводить Expected. 9 `.map_err(|e| TestCaseError::fail(e.to_string()))` → один `fail()`.

C7. **Фикстуры**: `kv()` в 6 файлах, `split()` в 2, `hex_decode`/`str_field` в 2,
    `len_field` в 6 → `tests/common/mod.rs` и `#[cfg(test)] mod test_util`. Bench: 95-строчный
    ручной argv-парсер при clap в workspace; `MINIMUM_MEASURED`, `TOKEN_BYTES` определены дважды.

## Часть D. Python-харнесс

D1. **Сделать директорию пакетом и убить dual-import идиому** (25 мест). Причина циклов:
    библиотечный код живёт в CLI-модуле `measure.py` (`run_case`, `prepare_build`,
    `EnginePhase`, `write_index`) и в тест-модуле `test_e2e.py` (~1800 из 3744 строк:
    `Engine`, `DockerStore`, `AlloyProducer`, `scan_objects`, `engine_metrics`).
    `performance._command()` существует только чтобы поздно импортировать `measure`.

D2. **Целевая раскладка** (deslop5-python.md, «Proposed target layout»): `harness/`
    с `proc.py` (run/docker/docker_json/popen_pinned/tail), `poll.py` (wait_until в секундах,
    `PeriodicSampler`), `engine.py`, `stores.py`, `producers.py`, `readers.py`, `controls.py`,
    `lifetime.py` (`EnginePhase`, слитый с `memory.Lifetime`), `results.py`
    (`check_hard`, `failed_names`, `rollup`, `write_family_index`, `next_ordinal`),
    `families/{local,stages,attribution,memory,faults/}`. `measure.py` становится
    листовым CLI ~200 строк с dispatch-таблицей. `test_e2e.py` — только TestCase'ы.

D3. **Дубли скаффолдинга**, копии → одна реализация: 4 конвенции subprocess (dict / tuple /
    JSON-or-None / сырые вызовы ×32); 3 семейства docker run/exec/rm; 4 механизма CPU-pinning;
    3 одинаковых sampler-потока; 6 копий хореографии engine-lifetime (allocate → register →
    snapshot → watch → send → drain → snapshot → unwatch); 5 сканеров ординалов
    (`memory._next` уже generic); 20+ ручных `while time.monotonic() < deadline` при
    существующем `wait_until` плюс `FaultRig._wait` как его копия; 3 варианта log tail.

D4. **Result-boilerplate**: тернарник `PASSED if ok else FAILED` ×47, comprehension
    «failed names» ×6, roll-up статуса ×9, тройка environment start/end/match ×8. Три
    хелпера и один `write_family_index` сокращают `publish_stages` 202→~60 и
    `publish_attribution` 208→~80. Риск: published-схема должна остаться байт-совместимой
    (`validate_result` и baseline-fingerprints это охраняют).

D5. **Разрезать семь функций 200-516 строк** по естественным швам: `exercise_outage`
    (516 строк, nesting 6, 79 локальных) → `run_producers_through_outage` /
    `assert_recovery` / `count_duplicates`; `reconcile_attribution` → `reconcile_row`;
    `attribution_lifetime` (10 параметров) → dataclass; `settle_pair`, `aggregate_attribution`
    → metrics / checks / observations.

D6. **Тестовый шум**: 309 преамбул (см. A1); `test_measurement.py` 6061 строк, 31 класс,
    организован по истории задач («Task 3c harness minors»), 73 `mock.patch`, 31 `assertIn`
    по свободному тексту → разрезать по модулям `harness/`; 6 stub-подкоманд CLI
    (`capacity`, `soak`, `failures`, `buffered`, `remediate`, `report`) возвращают 2 и
    «implemented by a later task» → удалить; `_ = call()` ×285 без линтера → sed.
    **Починить `LEGACY_TEST_COUNT = 18`** (measure.py:953; тестов 19; README тоже говорит 18).
    Константы `READY/DRAIN/SHUTDOWN_DEADLINE_S` определены в трёх модулях; два разных
    `TOPOLOGIES` под одним именем в measurement.py и test_e2e.py.

## Порядок выполнения

1. **Свип A1 + A4 + A5 + C5** (механика, нулевой риск поведения, минус ~3500 строк).
   Делать первым: после него остальной код читается и ревьюится в разы быстрее.
2. **A2 + A3 + A6 + A7** (проза; сначала удалить `readme_states_operating_contract`).
3. **B1, B2, B10, B11, B13, B14, B15** (S-эффорт, локальные, низкий риск).
4. **C1, C2, C3, C4, C6, C7** (тестовая инфраструктура; делать до B6/B12, чтобы рефакторинг
   production-кода шёл на компактной тестовой базе).
5. **B3, B4, B6, B8, B12** (M-эффорт, покрыты oracle/fuzz/sink cancellation тестами).
6. **B5, B7, B9, B16** (cross-crate интерфейсы; B9 меняет пользовательские сообщения).
7. **D1 → D2 → D3/D4 → D5/D6** (Python; D1 обязателен перед всем остальным, иначе
   нельзя чисто разделить харнесс и E2E). Баг `LEGACY_TEST_COUNT` править сразу.

Что **не** трогать: `cache.rs` (чистый), `canonical.rs` кроме плейсхолдер-доков,
`UploadLedger`+`CreationWatch` (закрывают реальные гонки), oracle-модель (не тавтологична,
не вызывает extract), доменные имена тестов (длинные, но информативные).

## Сквозные привычки, которые породили слоп

- Каждый doc-комментарий спорит: инвариант → контрфактуал («without this … would …») →
  выигрыш («so a producer never …»). Оставить первое предложение.
- Одно объяснение на каждом слое: module doc, type doc, fn doc, inline, test preamble, README.
- Параллельные списки, синхронизируемые руками: Outcome ×4, схема валидатора ×2,
  число колонок series ×3, лимиты блока ×2, правила валидации ×2.
- Ручные Arrow-чтения на каждом месте вместо одного generic; одно уже разошлось.
- Хелпер пишется в том файле, где понадобился впервые (test_e2e как библиотека,
  8 ObjectStore-обёрток, 5 сканеров ординалов, 4 subprocess-конвенции), потом
  копируется, а не импортируется.
- Test-seams и test-only поля в production-типах вместо ассертов по наблюдаемому выходу.
- Артефакты процесса в отгружаемом тексте: «spec section N», «plan 3», «Task 3c»,
  «planned», «still in progress», revision history неотгруженного формата.
