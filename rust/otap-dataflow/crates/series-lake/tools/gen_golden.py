#!/usr/bin/env python3
"""Independent implementation of canonical encoding v1 (spec section 4).

Generates tests/golden/canonical_v1.json. Requires: pip install xxhash
"""
import json, struct, sys
import xxhash

TAG = dict(str=1, bytes=2, int=3, double=4, bool=5, null=6, array=7, kvlist=8)
CANONICAL_NAN = 0x7FF8000000000000


def put(tag, payload):
    return bytes([tag]) + struct.pack(">I", len(payload)) + payload


def enc_str(s):
    return put(TAG["str"], s.encode("utf-8"))


def enc_value(v):
    t = v["type"]
    if t == "null":
        return put(TAG["null"], b"")
    if t == "str":
        return enc_str(v["value"])
    if t == "bytes":
        return put(TAG["bytes"], bytes.fromhex(v["value"]))
    if t == "int":
        return put(TAG["int"], struct.pack(">q", v["value"]))
    if t == "double":
        bits = v["bits"] if "bits" in v else struct.unpack(">Q", struct.pack(">d", v["value"]))[0]
        exp = bits >> 52 & 0x7FF
        mant = bits & ((1 << 52) - 1)
        if exp == 0x7FF and mant != 0:
            bits = CANONICAL_NAN
        return put(TAG["double"], struct.pack(">Q", bits))
    if t == "bool":
        return put(TAG["bool"], bytes([1 if v["value"] else 0]))
    if t == "array":
        payload = struct.pack(">I", len(v["items"])) + b"".join(enc_value(i) for i in v["items"])
        return put(TAG["array"], payload)
    if t == "kvlist":
        return enc_kvlist(v["entries"])
    raise ValueError(t)


def enc_kvlist(entries):
    entries = sorted(entries, key=lambda e: e["key"].encode("utf-8"))
    payload = struct.pack(">I", len(entries))
    for e in entries:
        payload += enc_str(e["key"]) + enc_value(e["value"])
    return put(TAG["kvlist"], payload)


def canonical(d):
    out = enc_str("OTEL-SERIES/1") + enc_str(d["signal"])
    out += enc_kvlist(d.get("resource_attrs", []))
    out += enc_str(d.get("resource_schema_url", ""))
    out += enc_str(d.get("scope_name", ""))
    out += enc_str(d.get("scope_version", ""))
    out += enc_str(d.get("scope_schema_url", ""))
    out += enc_kvlist(d.get("scope_attrs", []))
    has_metric = d["signal"] == "metrics"
    assert has_metric == ("metric" in d and d["metric"] is not None), \
        "metric must be present iff signal is metrics"
    if has_metric:
        m = d["metric"]
        out += enc_str(m["name"]) + enc_str(m.get("unit", "")) + enc_str(m["kind"])
        out += enc_str(m.get("temporality", "")) + put(TAG["bool"], bytes([1 if m.get("is_monotonic") else 0]))
    out += enc_kvlist(d.get("attrs", []))
    return out


def kv(key, value):
    return {"key": key, "value": value}


S = lambda s: {"type": "str", "value": s}
I = lambda i: {"type": "int", "value": i}
# D takes a finite Python float; non-finite doubles must use DB(bits) so that
# json.dump(allow_nan=False) never emits bare Infinity/NaN, which serde_json rejects.
D = lambda f: {"type": "double", "value": f}
DB = lambda bits: {"type": "double", "bits": bits}
B = lambda hexs: {"type": "bytes", "value": hexs}
BOOL = lambda b: {"type": "bool", "value": b}
NULL = {"type": "null"}
ARR = lambda *items: {"type": "array", "items": list(items)}
KV = lambda *entries: {"type": "kvlist", "entries": list(entries)}

base_logs = dict(signal="logs", resource_attrs=[kv("host.id", S("a1"))], scope_name="lib", scope_version="1")
base_metrics = dict(signal="metrics", resource_attrs=[kv("host.id", S("a1"))],
                    metric=dict(name="cpu.usage", unit="s", kind="gauge", temporality="", is_monotonic=False))

cases = [
    ("logs_minimal", base_logs),
    ("empty_string_attr", {**base_logs, "attrs": [kv("k", S(""))]}),
    ("missing_scope", {**base_logs, "scope_name": "", "scope_version": ""}),
    ("int_zero", {**base_logs, "attrs": [kv("k", I(0))]}),
    ("neg_zero_double", {**base_logs, "attrs": [kv("k", DB(0x8000000000000000))]}),
    ("int_min", {**base_logs, "attrs": [kv("k", I(-2**63))]}),
    ("int_max", {**base_logs, "attrs": [kv("k", I(2**63 - 1))]}),
    ("int_42", {**base_logs, "attrs": [kv("k", I(42))]}),
    ("str_42", {**base_logs, "attrs": [kv("k", S("42"))]}),
    ("nan_quiet", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000000))]}),
    ("nan_payload", {**base_logs, "attrs": [kv("k", DB(0x7FF8000000000001))]}),
    ("nan_negative", {**base_logs, "attrs": [kv("k", DB(0xFFF8000000000000))]}),
    # pos_inf / neg_inf: DB(...) bit patterns, not D(math.inf) / D(-math.inf),
    # so json.dump(..., allow_nan=False) never has to serialize a bare float
    # infinity (serde_json rejects bare Infinity/NaN in JSON text).
    ("pos_inf", {**base_logs, "attrs": [kv("k", DB(0x7FF0000000000000))]}),
    ("neg_inf", {**base_logs, "attrs": [kv("k", DB(0xFFF0000000000000))]}),
    ("unicode_bmp", {**base_logs, "attrs": [kv("k", S("\u00e9\u4e2d"))]}),
    ("unicode_supplementary", {**base_logs, "attrs": [kv("k", S("\U0001F600"))]}),
    ("embedded_nul", {**base_logs, "attrs": [kv("k", S("a\u0000b"))]}),
    ("bytes", {**base_logs, "attrs": [kv("k", B("00ff10"))]}),
    ("null_value", {**base_logs, "attrs": [kv("k", NULL)]}),
    ("bool_true", {**base_logs, "attrs": [kv("k", BOOL(True))]}),
    ("nested_array", {**base_logs, "attrs": [kv("k", ARR(I(1), S("x"), ARR(NULL)))]}),
    ("nested_kvlist", {**base_logs, "attrs": [kv("k", KV(kv("b", I(2)), kv("a", KV(kv("z", BOOL(False))))))]}),
    ("key_order_prefix", {**base_logs, "attrs": [kv("ab", I(1)), kv("a", I(2)), kv("a\u00e9", I(3)), kv("b", I(4))]}),
    ("metrics_gauge", base_metrics),
    ("metrics_sum_delta_monotonic", {**base_metrics, "metric": dict(name="req", unit="1", kind="sum", temporality="delta", is_monotonic=True)}),
    ("metrics_histogram_cumulative", {**base_metrics, "metric": dict(name="lat", unit="ms", kind="histogram", temporality="cumulative", is_monotonic=False)}),
    ("metrics_dp_attrs", {**base_metrics, "attrs": [kv("cpu", I(3)), kv("mode", S("user"))]}),
    ("metrics_scope_attrs", {**base_metrics, "scope_attrs": [kv("s", S("v"))], "scope_name": "m", "scope_version": "2"}),
    ("producer_a", {**base_logs, "resource_attrs": [kv("host.id", S("a"))]}),
    ("producer_b", {**base_logs, "resource_attrs": [kv("host.id", S("b"))]}),
    ("schema_urls", {**base_logs, "resource_schema_url": "https://r", "scope_schema_url": "https://s"}),
]

vectors = []
for name, d in cases:
    b = canonical(d)
    vectors.append({"name": name, "descriptor": d, "canonical_hex": b.hex(),
                    "series_id_hex": xxhash.xxh3_128_hexdigest(b)})
with open(sys.argv[1], "w", encoding="utf-8") as fh:
    json.dump({"format": "canonical_v1", "vectors": vectors}, fh,
              indent=1, ensure_ascii=False, allow_nan=False)
print(f"wrote {len(vectors)} vectors")
