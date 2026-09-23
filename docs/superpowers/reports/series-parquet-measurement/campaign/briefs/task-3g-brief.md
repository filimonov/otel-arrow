### Task 3g: Never acknowledge a damaged OTLP body (fifth review, P1, 2026-09-23)

**Defect, verified at HEAD:** `OtlpProtoBytes::validate_framing` (crates/pdata/src/otlp/mod.rs, calling `validate_message_wire_format` in crates/pdata/src/views/otlp/bytes/decode.rs) checks only the outer message ("without decoding nested messages"). The lazy view parser treats a damaged nested field as absent, so a body such as ExportLogsServiceRequest `[0x0a, 0x01, 0x0a]` passes validation, converts to zero rows, and worker.rs acknowledges it with nothing written. A damaged body must be refused (permanent, `Refused`, reason sentence names the damage) before any ack, and a partially damaged body must never be acknowledged with only its readable part stored.

- [ ] Reproduce first: a failing test in pdata with `[0x0a, 0x01, 0x0a]` for logs and an equivalent damaged metrics body, plus a failing exporter test that asserts the nack instead of the ack.
- [ ] Make validation schema-aware and recursive: descend into every LEN field that the OTLP schema defines as a sub-message (resource_logs -> resource, scope_logs -> scope, log_records -> attributes/body AnyValue, and the metrics tree down to data points, exemplars and AnyValue/KeyValue lists), validating each nested message's wire framing; unknown fields keep proto3 skip semantics; bound recursion depth by the existing nesting limit. Keep it allocation-free and linear in the body size.
- [ ] Every exporter that calls validate_framing gets the deeper check automatically; add one regression per exporter call site that was covered before.
- [ ] Measure the extra cost on the stage bench (otlp_convert) and record it; the check must stay a small fraction of conversion.
- [ ] chloggen bug_fix for pdata (upstream-relevant); ASCII; Scenario/Guarantees.

