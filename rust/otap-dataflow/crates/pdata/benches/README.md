# OTAP Benchmarks

Benchmarks for various OTAP operations.

## Concatenation

```bash
cargo bench --bench concatenate
```

## OTLP framing

The framing walk of `OtapPayload::validate_otlp_framing`, under both
`RepeatedSingular` policies, beside the OTLP-to-OTAP conversion on the same
logs, metrics and traces bodies.

```bash
cargo bench --bench otlp_framing
```
