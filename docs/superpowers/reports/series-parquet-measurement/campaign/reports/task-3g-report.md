# Task 3g report: never acknowledge a damaged OTLP body

Worktree: <repo>/.claude/worktrees/agent-a52116d451bc6bbbd
Branch: worktree-agent-a52116d451bc6bbbd (based on series-parquet-exporter at d141e8a49)
Commit: ded42cc85

## Reproduction (red runs, before the fix)

All new tests were written first and run against the unchanged
`validate_framing` (top-level walk only):

- pdata `otlp::tests::validate_framing_refuses_damage_inside_a_nested_message`
  FAILED: "Logs: a body damaged inside ResourceLogs was accepted" --
  `[0x0a, 0x01, 0x0a]` passed validation.
- series_parquet `a_body_damaged_inside_a_nested_message_is_refused_not_acked`
  FAILED: the worker emitted `DeliverAck` for the logs body `[0x0a, 0x01, 0x0a]`
  (zero rows, acknowledged as stored) -- the defect exactly as described.
- series_parquet `nesting_beyond_the_framing_bound_is_refused_as_too_deep`
  FAILED: 257 levels of nested arrays passed `validate_framing`.
- otap exporter `a_malformed_otlp_body_is_nacked_permanently` FAILED: the
  nested-damaged body `[0x0a, 0x01, 0x0a]` was nacked as retryable
  "export failed" (permanent: false), not refused as malformed.
- parquet exporter `a_malformed_otlp_body_is_not_written` FAILED -- by a
  PANIC: a request whose third record carries a string attribute whose
  AnyValue declares 5 bytes and holds 3 reached the OTLP -> OTAP encoder,
  which panicked at crates/pdata/src/encode/mod.rs:548
  (`val.as_string().expect("value to be string")`), taking down the exporter
  task. (A first version of this regression, damage appended inside
  `ResourceLogs`, happened to pass red, so it was replaced by the
  attribute-value damage.)

## Design of the recursive check

New module `crates/pdata/src/views/otlp/bytes/validate.rs`
(`pub(crate) fn validate_request(buf, root: Message)`), called by
`OtlpProtoBytes::validate_framing` with the root message chosen by the
variant (logs / metrics / traces request).

- A `Message` enum names every OTLP message type the walk knows; its
  `field(num) -> Field` table (built from `proto::consts::field_num`) says,
  per field number, whether it is a sub-message (`Message(child)`), a scalar
  with a fixed wire type (strings/bytes are `LEN`), a packable repeated scalar
  (`Packed(element wire type)`), or unknown.
- `walk(buf, base, message, depth)` loops over the fields of one message:
  decode the key (well-formed varint, <= u32::MAX, field number != 0), bound
  the value by its wire type (varint terminates, fixed64/fixed32 fit,
  LEN length fits the enclosing message; group wire types 3/4 and 6/7 are
  refused), then:
  - known sub-message: must be `LEN`, then recurse into exactly its byte range;
  - known scalar: wire type must match (prost refuses a mismatch; the lazy
    views would read garbage or drop it);
  - packed field: either the element wire type (unpacked) or `LEN` whose
    payload is a whole number of fixed64 elements / only terminated varints;
  - unknown field: framing checked, content skipped (proto3 semantics).
- Depth bound: `pub const MAX_ANY_VALUE_NESTING_DEPTH = 256`, counted in
  AnyValue container levels (entering an `ArrayValue` or `KeyValueList` is one
  level) -- the same unit as series-lake's `ingress.max_nesting_depth` (CBOR
  container levels) and equal to its largest accepted value
  (`lake::config::MAX_NESTING_DEPTH = 256`). A compile-time assertion in
  worker.rs keeps pdata's bound >= the lake's cap, so the walk never refuses a
  body a configured limit would accept. Beyond it: `Error::OtlpNestingTooDeep`.
  The schema's fixed levels are at most 9 deep, so recursion is bounded by
  9 + 3 * 256 frames; the test at exactly 256 levels runs on a debug test
  thread's stack.
- Errors: new `pdata::error::Error::InvalidOtlpWireFormat { problem, message,
  offset }` ("Invalid protobuf wire format: length-delimited field overruns
  its message in AnyValue at byte 13") and `OtlpNestingTooDeep { limit,
  offset }`. Internally the walk returns a small `Damage` value and only the
  outermost call builds the crate error. `InvalidProtobufWireFormat` stays as
  is for `Raw*Data::try_new`, which keeps its documented top-level-only check
  (callers: console, geneva, clickhouse, pdata-codec) -- unchanged.
- Cost model: no allocation; each byte is parsed once, by the innermost
  message that holds it, so the walk is linear in the body size.

Exporter side (series_parquet worker.rs `check_wire_format`): a framing error
is still `Refused` as `invalid request content: malformed OTLP <signal> body:
<error>`; `OtlpNestingTooDeep` is mapped to
`RefuseReason::TooDeep(configured max_nesting_depth)`, so a body refused by the
walk and one refused by the lake after conversion read the same reason.
Module docs, the `check_wire_format` doc and the README "Validation not
performed in v1" bullets were updated (nested damage is now refused; the
recursive value encoder is now bounded at 256 levels for OTLP bytes).

## Message types covered

Logs: ExportLogsServiceRequest, ResourceLogs, ScopeLogs, LogRecord (body
AnyValue, attributes).
Metrics: ExportMetricsServiceRequest, ResourceMetrics, ScopeMetrics, Metric
(incl. metadata), Gauge, Sum, Histogram, ExponentialHistogram, Summary,
NumberDataPoint, HistogramDataPoint (packed bucket_counts / explicit_bounds),
ExponentialHistogramDataPoint, its Buckets (packed varint bucket_counts),
SummaryDataPoint, ValueAtQuantile, Exemplar.
Traces (also done, since the file, parquet and otap exporters validate traces
bodies too): ExportTraceServiceRequest, ResourceSpans, ScopeSpans, Span,
Span.Event, Span.Link, Status.
Shared: Resource (incl. entity_refs -> EntityRef), InstrumentationScope,
KeyValue, AnyValue, ArrayValue, KeyValueList.
New field-number constants: `common::ENTITY_REF_*`,
`resource::RESOURCE_ENTITY_REFS`.

Not checked (content, not framing): UTF-8 of strings, value ranges, enum
values.

## Measured cost

Direct timing on the stage-bench inputs (copies of
/tmp/series-memory-prefix-probes/inputs/{logs-1k-stable,metrics-mixed}.otlp),
one release binary (a throwaway example, not committed) timing, per request:
the old top-level walk (`Raw*Data::try_new`, identical to the previous
`validate_framing`), the new `validate_framing`, and the otlp_convert stage's
operation (`OtapPayload::try_into_with_default` to OTAP records). Pinned with
`taskset -c 0-7,16-23`, host measurement lease held for the build and the
runs, no compiler running.

| input | requests, bytes | before (top-level) | after (schema walk) | otlp_convert | added cost / convert |
|---|---|---|---|---|---|
| logs-1k-stable | 200, 21.6 MB | 0.01 us/req | 3.0-3.5 us/req (~32-36 GB/s) | 33-36 us/req | 9.5-9.6% |
| metrics-mixed | 150, 0.91 MB | 0.01 us/req | 1.9-2.5 us/req (~3.1 GB/s at best) | 56-57 us/req | 3.5-4.4% |

(ranges are best / median over 50 rounds for logs and 200 for metrics,
two runs each). Logs is the larger share because its conversion is cheap per
byte (1 KiB bodies copied whole) while the walk visits every record's fields;
per field the walk costs on the order of a nanosecond. Replacing the crate
error by a small internal error type did not change the time (kept for its
smaller recursive frames).

## Tests

- pdata: 738 lib tests pass (+2 doc/integration groups). New: 1 in
  otlp/tests.rs (the reproduction; the existing
  `validate_framing_refuses_a_damaged_body` updated to the new variant) and 7
  in validate.rs (fully populated logs/metrics/traces requests pass; every
  cut prefix is refused unless it ends on a top-level field boundary; deep
  damage names AnyValue and the exact byte offset; unknown fields of every
  wire type skipped but framed; wrong wire types and group/zero keys refused;
  packed fields must hold whole elements, unpacked accepted; nesting exact at
  256, refused at 257).
- core-nodes with `--features series-parquet`: 1194 lib tests pass (+1).
  New: series_parquet `a_body_damaged_inside_a_nested_message_is_refused_not_acked`,
  `nesting_beyond_the_framing_bound_is_refused_as_too_deep`; extended:
  parquet `a_malformed_otlp_body_is_not_written` (adds the attribute-damaged
  body), otap `a_malformed_otlp_body_is_nacked_permanently` (adds
  `[0x0a, 0x01, 0x0a]`). The file exporter had no damaged-body regression
  before and gets none (it calls the same `validate_framing`).
- series-lake: all tests pass.
- cargo fmt, clippy -D warnings (pdata + core-nodes with series-parquet,
  all targets): clean. `cargo xtask check`: see below.

`cargo xtask check` (fmt, clippy, full workspace tests): passed, exit 0, "All tests passed successfully" (after initializing the proto submodules in the worktree, see Concerns).

## Concerns

- The OTLP -> OTAP encoder panics on a damaged nested string AnyValue
  (crates/pdata/src/encode/mod.rs:548 `expect("value to be string")`, and the
  sibling `expect`s for int/double/bool). Exporters that call
  `validate_framing` first are now protected, but any other path that converts
  unvalidated OTLP bytes to OTAP (processors, other exporters) can still be
  crashed by a malformed request. Not fixed here (outside this task);
  worth its own task: either validate in the conversion entry point or make
  those `expect`s errors.
- Refusing a known field with a mismatched wire type is stricter than the C++
  and Go protobuf runtimes (which keep such a field as unknown) but matches
  prost; no conforming encoder produces it.
- The worktree needed `git submodule update --init proto/opentelemetry-proto
  proto/opamp-spec` for `cargo xtask check` (query-engine-playground
  include_str!s the protos); no tracked file changed.

## Commits

- ded42cc85 fix(pdata): validate the framing of every nested OTLP message (on branch worktree-agent-a52116d451bc6bbbd, parent d141e8a49). Not pushed.

## Fix round 1

Commit 696b81d02 "fix(pdata): refuse overflowing varints and invalid UTF-8,
skip balanced unknown groups" on top of ded42cc85 (branch
worktree-agent-a52116d451bc6bbbd). Not pushed.

1. Varint overflow (Critical). Fixed in the shared decoder
   `views::otlp::bytes::decode::read_varint`: a terminating tenth byte above
   0x01 now returns None (more than ten bytes already did). Other callers of
   this decoder: the pdata byte views (common, logs, metrics, traces
   parsers), `validate_message_wire_format` (Raw*Data::try_new) and
   `otlp::batching::next_field`; all treat None as damage (field absent /
   refused), so none reads a wrapped value any more. The KQL processor and
   telemetry encoder have their own decoders and are unaffected. The
   validator's messages are now "truncated or overlong ..." for keys,
   lengths, varint values and packed varints.
   Tests: decode.rs `refuses_varints_that_overflow_u64` (u64::MAX in 10 bytes
   decodes; `80x9 02`, `80x9 7f` and an 11-byte varint refused; top-level
   varint field refused); validate.rs `a_varint_that_overflows_u64_is_refused`
   (scalar, length prefix, field key and packed Buckets.bucket_counts, max
   u64 accepted in the scalar and packed cases).
2. UTF-8 (Important). New `Field::Str` for every `string` field (schema_url,
   severity_text, event_name, metric name/description/unit, KeyValue.key,
   AnyValue.string_value, scope name/version, EntityRef fields, span
   trace_state/name, event name, link trace_state, status message); `bytes`
   fields (trace/span ids, AnyValue.bytes_value, exemplar ids) stay
   unchecked. Checked with `std::str::from_utf8` on the slice, no
   allocation. Test `a_string_field_must_hold_valid_utf8`: string_value
   `[0xff]` refused, a multibyte string (2-, 3- and 4-byte sequences)
   accepted, bytes_value `[0xff]` accepted, KeyValue.key `[0xff]` and a
   truncated two-byte sequence in Metric.name refused.
3. Unknown groups (Important). The walk now reads the key, then: an end
   group at message level is refused ("end group without a start group"); a
   start group on a known field is refused (wrong wire type); a start group on
   an unknown field is skipped by `skip_group`, which frames every field
   inside, recurses into nested groups, requires the end key of the same field
   number ("end group does not match its start group") before the message
   ends ("group without an end group"), and counts each group level against
   MAX_ANY_VALUE_NESTING_DEPTH (256 nested groups accepted, 257 refused as
   OtlpNestingTooDeep). Test `unknown_groups_are_skipped_when_balanced`:
   field 31 as `fb 01 fc 01` accepted, with every other wire type inside
   accepted, with a nested field-32 group accepted; stray end, mismatched
   end, unclosed group, group on LogRecord field 1 refused; depth exact.
4. File exporter (Minor): `nested_damage_is_refused_before_framing` --
   logs/metrics/traces `0a 01 0a` refused by `encode_payload`, the error names
   ResourceLogs/ResourceMetrics/ResourceSpans, the reusable frame is cleared.

The new tests were written against behaviour the round-0 code lacked (it
accepted each refused input); no separate red run was made to keep the host
lease short.

Tests: pdata 742 lib tests pass (+4), core-nodes with series-parquet 1195
(+1), series-lake all pass; fmt, clippy -D warnings (pdata, core-nodes with
series-parquet, all targets) clean; `cargo xtask check` passed (exit 0).

Re-measured cost (same throwaway release example, same inputs,
`taskset -c 0-7,16-23`, host lease held):

| input | before (top-level) | round 0 | round 1 | otlp_convert | round 1 share |
|---|---|---|---|---|---|
| logs-1k-stable | 0.01 us/req | 3.0-3.5 us/req | 5.0-6.4 us/req | 32-42 us/req | 13.5-18.5% |
| metrics-mixed | 0.01 us/req | 1.9-2.5 us/req | 3.4-3.5 us/req | 56-57 us/req | 6.1% |

The rise is the UTF-8 pass over the ~1 KiB log bodies (and metric/attribute
strings). The conversion validates the same strings again when it turns
binary into string arrays (lossy, crates/pdata/src/encode/record/array.rs), so
a follow-up could let the conversion trust input `validate_framing` already
accepted and win this back; simdutf8 (already in Cargo.lock transitively)
would be the other lever. Neither done here.

Lease etiquette note: my holder took the lease in a gap between two
repetitions of another agent's `measure memory` campaign (pid 1194549), which
then waited about 18 minutes for my build, test, measurement and xtask run
before resuming (its r004 started 13:17:57Z). No build overlapped a
measurement.

## Fix round 2

Commit 1b7d3aaf6 "fix(pdata): byte views step over every field the validator
accepts" on top of 696b81d02 (branch worktree-agent-a52116d451bc6bbbd). Not
pushed.

The ruling was followed: the byte-view scanners skip balanced groups with
the validator's own helper, which is bounded, so the fallback (refusing
unknown groups) was not needed.

- Shared helpers in decode.rs: `skip_group` (the only group skipper, bounded
  by MAX_ANY_VALUE_NESTING_DEPTH, used by the validator and the scanners),
  `field_range(buf, tag, pos)` (any field's range, groups included, for
  scanners), and `read_key` / `value_range` moved from validate.rs;
  `field_value_range` is now `value_range(..).ok()`, so one function decides
  framing for the validator and the views.
- Group skip added to ProtoBytesParser::advance_to_find_field,
  RepeatedFieldProtoBytesParser (both scans), RepeatedPrimitiveIter and
  otlp::batching::next_field.
- Further view defects the new end-to-end test found, all the same class
  (content the validator accepts, silently lost or misread by the views),
  fixed here:
  - the request-level ResourceLogs/ResourceMetrics/ResourceSpans iterators
    and the ArrayValue iterator skipped only a non-matching field's key and
    read its value as keys (unknown bytes `0a 00` became a phantom empty
    resource); they now step over the whole field with `field_range`;
  - RawKeyValue read every field as length-delimited; it now steps over each
    by its own wire type and reads key/value only when LEN;
  - RawAnyValue read only the first field; it now scans all fields and the
    last oneof member wins, as prost decodes it, so an unknown field before
    the value no longer turns it into an empty value;
  - the span FieldRanges table was indexed by field number and PANICKED
    (index out of bounds) on any unknown span field above 16 -- a newer OTLP
    field would have crashed the view; such fields are now ignored.
  `validate_message_wire_format` (Raw*Data::try_new, top-level only) still
  refuses groups, unchanged.

Tests (red first: with the scanner group skip disabled, both failed -- the
series_parquet test showed the decorated logs request extracted with 1 row
but `resource_attrs: []` instead of `service.name = checkout`):
- pdata validate.rs `unknown_content_before_known_fields_is_skipped_by_the_views`:
  each fully populated logs/metrics/traces request, re-encoded with an
  unknown balanced group (with a nested group), an unknown LEN field whose
  bytes look like a known field, and an unknown varint before the first field
  of EVERY message (request, resource, scope, record, metric, each data point
  kind, exemplar, buckets, quantile, span, event, link, status, entity ref,
  key-value, any-value, array, list), passes validation and converts to
  exactly the OTAP records of the plain request.
- series_parquet `an_unknown_group_before_known_fields_loses_nothing`: logs
  and metrics requests with an unknown group before the first known field of
  the resource, the log record and the gauge data point are admitted with the
  same stats (rows 1), descriptors and values as the plain request; the
  resource attribute, log body and attribute, point value 42 and attribute
  are present.

Test counts: pdata 743 lib (+1), core-nodes with series-parquet 1196 (+1),
series-lake all pass; fmt and clippy -D warnings clean; `cargo xtask check`
passed.

Cost check (same throwaway example, lease held): validator 5.4-6.7 us per
logs-1k-stable request (19% of conversion), 4.1-4.2 us per metrics-mixed
request (7%); conversion itself 33-35 us (logs) and 58.8-59.5 us (metrics),
i.e. unchanged within run-to-run noise for logs and about 4% higher for
metrics (the AnyValue full scan is a candidate; not investigated further).

Lease note: one `cargo build -p otel-arrow-dfe-pdata` in this round ran
without the lease, right when another agent's `measure memory` repetition
(memory-strict-bgthread-strict-local-c1-w1-r004, acquired 13:33:24Z) started;
its build monitor may have invalidated that repetition. Every later build,
test, measurement and xtask run held the lease (acquired 13:35:33Z).

## Fix round 3

Commit 0ec7e938b "fix(pdata): merge a repeated array or kvlist AnyValue
member as prost does" on top of 1b7d3aaf6 (branch
worktree-agent-a52116d451bc6bbbd). Not pushed.

Prost semantics implemented in the view (no validator fallback needed):
- `RawAnyValue::value_type` scan: for `array_value` / `kvlist_value`, a
  member that follows itself continues the current run and `value_offset`
  keeps the key position of the run's first occurrence; any other member
  starts a new run (last member wins across variants); scalar members keep
  pointing at the last value, as before.
- `as_array` / `as_kvlist` return `MergedMember<'a, I>`, a generic iterator
  that walks the AnyValue bytes from that key, opens each occurrence of the
  member (via `open_array` / `open_kvlist`) and chains their elements in
  order, stepping over unknown fields with the shared `field_range`. No
  allocation, no range list: occurrences are found by scanning forward as
  iteration reaches them, each byte read once. The `ArrayIter` and
  `KeyValueIter` associated types of `RawAnyValue` changed to
  `MergedMember<...>`; nothing names them concretely elsewhere.

Tests (both red with the merge disabled, i.e. with the round-2 last-only
behaviour):
- pdata common.rs `repeated_any_value_members_read_as_prost_decodes_them`:
  the view, read recursively into the prost type, equals prost's own
  decoding for: nonempty array then empty array (2 elements kept), two
  nonempty kvlists (3 pairs concatenated), array then string (the string),
  array/string/array (last array only), arrays around an unknown field
  (merged), string then array, two strings (last).
- series_parquet `a_repeated_any_value_member_is_merged_end_to_end`: a log
  record whose attribute repeats `array_value` (["alpha","beta"] then []) and
  whose body repeats `kvlist_value` ({k1:one} then {k2:two}) is admitted with
  the same stats (1 row), descriptors and values as the prost-decoded and
  re-encoded request, and alpha, beta, k1, one, k2, two are all stored.

Counts: pdata 744 lib (+1), core-nodes with series-parquet 1197 (+1),
series-lake all pass; fmt, clippy -D warnings clean; `cargo xtask check`
passed. All builds and tests ran under the host lease (acquired 13:59:56Z,
taken while it was free, released after xtask).

## Fix round 4

Commit 354fa2956 "fix(pdata): refuse OTLP bodies that repeat a singular
field or oneof" on top of 0ec7e938b (branch worktree-agent-a52116d451bc6bbbd).
Not pushed.

Scalar claim checked first, and disproved: a probe (temporary test, not
committed) converted a log record carrying time_unix_nano 1 then 2,
severity_text "first" then "second" and severity_number 5 then 9 through the
byte views and compared it with the prost-decoded, re-encoded request. Prost
decodes 2 / "second" / 9 (last wins); the views read the first occurrences.
`ProtoBytesParser::advance_to_find_field` stops at the first occurrence of a
field it is asked for, so for singular scalars the views are first-wins (or
last-wins only if another lookup scanned past both first). Accepting
repeated scalars would therefore also store data read differently from
prost, so the refusal covers them as well.

Implemented (validator only; the 12 views are unchanged, the AnyValue merge
stays):
- `Message::singular(num) -> Option<(slot, name)>` lists every singular
  field of every message the walk knows -- scalars, strings, bytes and
  sub-messages -- with oneof members sharing one slot (Metric.data 5/7/9/10/11,
  NumberDataPoint.value 4/6, Exemplar.value 3/6). AnyValue has none.
- `walk` keeps a `u32` bitmask of the slots seen in the current message (no
  allocation) and refuses a second occurrence as the new
  `Error::DuplicateOtlpField { message, field, offset }`:
  "OTLP ResourceLogs.resource occurs more than once in one message at byte
  N; the OTLP byte views would not read it as protobuf merges it". The
  exporters wrap it as before, e.g. series_parquet's
  "invalid request content: malformed OTLP logs body: OTLP
  ResourceLogs.resource occurs more than once ...", a permanent Refused.
- Repeated fields (attributes, data points, packed and unpacked numerics,
  entity ref keys, request-level resource_* lists) are unaffected.
- Docs corrected as asked: `open_kvlist` says each occurrence allocates one
  `Rc` through its `ProtoBytesParser`; `MergedMember` says the final run's
  keys and lengths are read a second time after `value_type`'s scan. The
  validator module doc and `validate_framing` doc name the new refusal.

Tests (red with the duplicate check disabled):
- pdata validate.rs `a_repeated_singular_field_is_refused`: walks the fully
  populated logs, metrics and traces requests, collects every singular field
  they set, and for each writes that field twice in every instance of its
  message: 118 cases over 26 message types, each refused as
  DuplicateOtlpField naming exactly that message and field (explicitly
  including ResourceLogs.resource, ScopeSpans.scope, LogRecord.body,
  Metric.data, ExponentialHistogramDataPoint.positive/negative, Span.status,
  KeyValue.value); both members of the NumberDataPoint and Exemplar value
  oneofs refused; the corpus with single occurrences passes; an AnyValue with
  string, string, array passes.
- series_parquet `a_repeated_singular_field_is_refused_not_acked`: a repeated
  ResourceLogs.resource and a metric with both gauge and sum are each nacked
  permanent, Refused, with a reason naming ResourceLogs.resource /
  Metric.data and "occurs more than once"; nothing is admitted.

Counts: pdata 745 lib (+1), core-nodes with series-parquet 1198 (+1; this
run covers the file, parquet, otap and series_parquet exporter tests); fmt,
clippy -D warnings clean; one final `cargo xtask check` passed. All builds,
tests and the xtask run were under the host lease (acquired 14:19:32Z while
free, released after xtask).
