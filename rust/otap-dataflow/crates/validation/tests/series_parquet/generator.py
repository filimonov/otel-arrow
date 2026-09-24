# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
"""Self-verifying requests built from templates, and the oracle that reads them.

Every record's identity is its sequence number, `request * records_per_request
+ position`, carried in its timestamp and, for logs, in the record id that
opens its body, so the stored rows can be checked against the acknowledged
requests by aggregates alone, without a per-record ledger.
"""
import hashlib
import random
import struct
import threading

try:  # Imported as a package module by `python3 -m crates...`.
    from . import measurement
except ImportError:  # Imported by path, e.g. from an ad hoc script.
    import measurement

test_e2e = measurement.test_e2e

GENERATOR_FORMAT = "series-template/1"
# Distinct templates per signal. A request uses template `index % VARIANTS`,
# so a record's padding and values repeat only every VARIANTS requests, a
# prime so no workload period aligns with it.
VARIANTS = 97
# Fixed widths of the rewritten digit fields.
REQUEST_DIGITS = 12
SLOT_DIGITS = 12
LOGGER_PREFIX = "series.logger."
_ALPHABET = measurement._PADDING.encode("ascii")
_TRANSLATE = bytes(_ALPHABET[i % len(_ALPHABET)] for i in range(256))
_U64 = struct.Struct("<Q")


def _stream(*parts, length) -> bytes:
    """`length` deterministic bytes from the named parts."""
    return hashlib.shake_256(":".join(str(part) for part in parts).encode("ascii")).digest(length)


def _unit(raw: bytes) -> float:
    """A float in [0, 1) from eight bytes."""
    return int.from_bytes(raw, "little") / 2**64


class TemplateRequests:
    """One workload's requests: a few templates, rewritten per request.

    A template is a complete OTLP request whose per-request fields -- the
    request digits of each record id, each timestamp and each series slot
    -- have fixed widths and known offsets; `request` copies the template
    for `index % VARIANTS` and writes those fields. Padding and point values
    come from the template, so they differ between the VARIANTS templates
    and repeat only every VARIANTS requests.
    """

    def __init__(self, workload):
        self.workload = workload
        self._templates = {}
        self._lock = threading.Lock()

    def slot(self, index, point) -> int:
        """The series slot of one record, as the workload defines it."""
        return self.workload.slot(index, point)

    def _template(self, signal, variant):
        key = (signal, variant)
        with self._lock:
            if key not in self._templates:
                builder = self._logs_template if signal == "logs" else self._metrics_template
                self._templates[key] = builder(variant)
            return self._templates[key]

    def _logs_template(self, variant):
        workload = self.workload
        rpr = workload.records_per_request
        request = test_e2e.logs_pb.ExportLogsServiceRequest()
        resource = request.resource_logs.add()
        resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
        resource.resource.attributes.add(key="service.name").value.string_value = (
            "series-e2e-service"
        )
        scope = resource.scope_logs.add()
        scope.scope.name = "series-e2e"
        width = workload.body_bytes - measurement.ID_FIXED_WIDTH - len(measurement.LOG_KIND)
        padding = _stream(workload.seed, "logs", variant, length=width * rpr).translate(_TRANSLATE)
        markers = []
        for point in range(rpr):
            record_id = measurement.stable_id(workload.seed, 0, point, measurement.LOG_KIND)
            body = record_id + padding[point * width:(point + 1) * width].decode("ascii")
            stamp = _marker_time(point)
            record = scope.log_records.add(time_unix_nano=stamp)
            record.body.string_value = body
            logger = f"{LOGGER_PREFIX}{point:0{SLOT_DIGITS}d}"
            record.attributes.add(key="logger.name").value.string_value = logger
            markers.append((record_id.encode("ascii"), stamp, logger.encode("ascii")))
        wire = request.SerializeToString(deterministic=True)
        # A record serializes its timestamp, then its body, then its
        # attributes, and records follow one another, so each field is found
        # after the one before it.
        fields = []
        at = 0
        for point, (record_id, stamp, logger) in enumerate(markers):
            at = _find_after(wire, _U64.pack(stamp), at)
            fields.append((at, 8, "time", point))
            at = _find_after(wire, record_id, at)
            fields.append((at + 9, REQUEST_DIGITS, "id", point))
            at = _find_after(wire, logger, at)
            fields.append((at + len(LOGGER_PREFIX), SLOT_DIGITS, "slot", point))
        return _compile(wire, fields, "logs")

    def _metrics_template(self, variant):
        workload = self.workload
        rpr = workload.records_per_request
        request = test_e2e.metrics_pb.ExportMetricsServiceRequest()
        resource = request.resource_metrics.add()
        resource.resource.attributes.add(key="host.id").value.string_value = "producer-1"
        resource.resource.attributes.add(key="service.name").value.string_value = (
            "series-e2e-service"
        )
        scope = resource.scope_metrics.add()
        scope.scope.name = "series-e2e"
        metrics = {
            kind: scope.metrics.add(name=measurement.METRIC_NAME_PREFIX + kind, unit="1")
            for kind in measurement.METRIC_KINDS
        }
        raw = _stream(workload.seed, "metrics", variant, length=64 * rpr)
        markers = []
        for point in range(rpr):
            kind = measurement.METRIC_KINDS[point % len(measurement.METRIC_KINDS)]
            draw = raw[point * 64:(point + 1) * 64]
            stamp = _marker_time(point)
            metric = metrics[kind]
            if kind in ("gauge_int", "gauge_double"):
                data = metric.gauge.data_points.add(time_unix_nano=stamp)
            elif kind in ("sum_int", "sum_double"):
                metric.sum.aggregation_temporality = 2
                metric.sum.is_monotonic = True
                data = metric.sum.data_points.add(time_unix_nano=stamp)
            else:
                metric.histogram.aggregation_temporality = 2
                if kind == "hist":
                    buckets = [1 + draw[8 + i] % 97 for i in range(4)]
                    bounds = sorted(round(_unit(draw[16 + 8 * i:24 + 8 * i]) * 1e6, 3) + i
                                    for i in range(3))
                else:
                    buckets, bounds = [], []
                data = metric.histogram.data_points.add(
                    time_unix_nano=stamp,
                    count=sum(buckets) if buckets else 1 + int.from_bytes(draw[8:10], "little"),
                    sum=round(_unit(draw[40:48]) * 1e9, 3),
                )
                data.bucket_counts.extend(buckets)
                data.explicit_bounds.extend(bounds)
            if kind in ("gauge_int", "sum_int"):
                data.as_int = int.from_bytes(draw[:8], "little", signed=True) >> 2
            elif kind in ("gauge_double", "sum_double"):
                data.as_double = round(_unit(draw[:8]) * 1e9, 3)
            slot = f"{point:0{SLOT_DIGITS}d}"
            data.attributes.add(key="series.slot").value.string_value = slot
            markers.append((stamp, slot.encode("ascii")))
        wire = request.SerializeToString(deterministic=True)
        # Points serialize metric by metric, each metric's points in order,
        # a point's timestamp (field 3) before its attributes.
        order = sorted(range(rpr), key=lambda point: (point % len(measurement.METRIC_KINDS),
                                                      point))
        fields = []
        at = 0
        for point in order:
            stamp, slot = markers[point]
            at = _find_after(wire, _U64.pack(stamp), at)
            fields.append((at, 8, "time", point))
            at = _find_after(wire, b"series.slot\x12\x0e\n\x0c" + slot, at)
            fields.append((at + 15, SLOT_DIGITS, "slot", point))
        return _compile(wire, fields, "metrics")

    def request(self, index):
        """The signal and wire bytes of request `index`: the template's
        fixed parts joined with this request's fields."""
        signal = self.workload.signal_of(index)
        template = self._template(signal, index % VARIANTS)
        rpr = self.workload.records_per_request
        base = index * rpr
        times = struct.pack(f"<{rpr}Q", *(time_of(base + point) for point in range(rpr)))
        digits = b"%012d" % index
        values = {
            "time": lambda point: times[point * 8:point * 8 + 8],
            "id": lambda point: digits,
            "slot": lambda point: b"%012d" % self.slot(index, point),
        }
        parts = template["statics"]
        out = [None] * (2 * len(template["fields"]) + 1)
        out[0::2] = parts
        out[1::2] = [values[kind](point) for kind, point in template["fields"]]
        return signal, b"".join(out)

    def size(self, index) -> int:
        """The wire size of request `index`: its template's."""
        signal = self.workload.signal_of(index)
        return self._template(signal, index % VARIANTS)["size"]

    def as_json(self) -> dict:
        return {"format": GENERATOR_FORMAT, "variants": VARIANTS,
                "workload": self.workload.as_json()}


def _marker_time(point) -> int:
    """A placeholder timestamp no other point of a template shares."""
    return measurement.LOG_BASE_TIME_NS + (10**9 + point) * measurement.METRIC_TIME_STEP_NS


def time_of(seq) -> int:
    """A record's timestamp: its sequence number on the harness time base."""
    return measurement.LOG_BASE_TIME_NS + seq * measurement.METRIC_TIME_STEP_NS


def _find_after(data, pattern, start) -> int:
    """The first offset of `pattern` in `data` at or after `start`."""
    at = data.find(pattern, start)
    if at < 0:
        raise AssertionError(f"template field {pattern!r} not found")
    return at


def _compile(wire, fields, signal) -> dict:
    """A template as its fixed parts and the ordered fields between them."""
    fields = sorted(fields)
    statics = []
    at = 0
    for offset, width, _kind, _point in fields:
        if offset < at:
            raise AssertionError("template fields overlap")
        statics.append(wire[at:offset])
        at = offset + width
    statics.append(wire[at:])
    return {"statics": statics, "fields": [(kind, point) for _o, _w, kind, point in fields],
            "size": len(wire), "signal": signal}


# --------------------------------------------------------------------------
# The aggregate oracle
# --------------------------------------------------------------------------

SAMPLE_RECORDS = 200


def _seq(column="v.time_unix_nano") -> str:
    """A stored row's sequence number, from its timestamp."""
    return (
        f"((CAST({column} AS HUGEINT) - {measurement.LOG_BASE_TIME_NS}) "
        f"// {measurement.METRIC_TIME_STEP_NS})"
    )


def request_coverage(db, relation, rpr, acked, failed) -> dict:
    """Lost, duplicated and foreign records of one signal, per request.

    `relation` is a SQL relation with a `seq` column. Every acknowledged
    request must hold its `rpr` sequence numbers exactly once; a stored
    request that was neither acknowledged nor failed is foreign; a failed
    request may be stored (at-least-once) and is counted apart.
    """
    for name, indexes in (("acked_requests", acked), ("failed_requests", failed)):
        db.execute(f"CREATE OR REPLACE TEMP TABLE {name} (req BIGINT)")
        if indexes:
            db.execute(f"INSERT INTO {name} SELECT unnest(?::BIGINT[])", [list(indexes)])
    db.execute(
        "CREATE OR REPLACE TEMP TABLE stored AS SELECT seq // ? AS req, count(*) AS c, "
        "count(DISTINCT seq) AS d, min(seq % ?) AS lo, max(seq % ?) AS hi "
        f"FROM {relation} GROUP BY 1", [rpr, rpr, rpr],
    )
    lost_requests, lost_records = db.execute(
        "SELECT count(*), coalesce(sum(? - coalesce(s.d, 0)), 0) FROM acked_requests a "
        "LEFT JOIN stored s ON s.req = a.req WHERE s.req IS NULL OR s.d < ?", [rpr, rpr],
    ).fetchone()
    duplicated = db.execute("SELECT coalesce(sum(c - d), 0) FROM stored").fetchone()[0]
    foreign, foreign_records = db.execute(
        "SELECT count(*), coalesce(sum(c), 0) FROM stored s WHERE s.req NOT IN "
        "(SELECT req FROM acked_requests) AND s.req NOT IN (SELECT req FROM failed_requests)"
    ).fetchone()
    out_of_range = db.execute(
        "SELECT count(*) FROM stored WHERE lo < 0 OR hi >= ?", [rpr]
    ).fetchone()[0]
    stored_failed = db.execute(
        "SELECT count(*), coalesce(sum(c), 0) FROM stored s WHERE s.req IN "
        "(SELECT req FROM failed_requests)"
    ).fetchone()
    count, distinct, low, high, total = db.execute(
        f"SELECT count(*), count(DISTINCT seq), min(seq), max(seq), sum(seq) FROM {relation}"
    ).fetchone()
    expected_count = len(acked) * rpr
    expected_sum = sum(r * rpr * rpr + rpr * (rpr - 1) // 2 for r in acked)
    return {
        "acknowledged_requests_count": len(acked),
        "expected_record_count": expected_count,
        "stored_record_count": int(count),
        "distinct_record_count": int(distinct),
        "seq_min": int(low) if low is not None else None,
        "seq_max": int(high) if high is not None else None,
        "seq_sum": int(total) if total is not None else 0,
        "expected_seq_sum_of_acknowledged": expected_sum,
        "lost_requests_count": int(lost_requests),
        "missing_record_count": int(lost_records),
        "duplicate_record_count": int(duplicated),
        "foreign_requests_count": int(foreign),
        "unexpected_record_count": int(foreign_records),
        "out_of_range_positions_count": int(out_of_range),
        "stored_failed_requests_count": int(stored_failed[0]),
        "stored_failed_records_count": int(stored_failed[1]),
    }


def expected_records(generator, index) -> dict:
    """Every record of request `index` as the generator wrote it, by seq."""
    signal, wire = generator.request(index)
    rpr = generator.workload.records_per_request
    records = {}
    if signal == "logs":
        request = test_e2e.logs_pb.ExportLogsServiceRequest.FromString(wire)
        for point, record in enumerate(request.resource_logs[0].scope_logs[0].log_records):
            records[index * rpr + point] = {
                "time_unix_nano": record.time_unix_nano,
                "body": record.body.string_value,
                "logger": record.attributes[0].value.string_value,
            }
        return records
    request = test_e2e.metrics_pb.ExportMetricsServiceRequest.FromString(wire)
    for metric in request.resource_metrics[0].scope_metrics[0].metrics:
        kind = metric.name.removeprefix(measurement.METRIC_NAME_PREFIX)
        points = (metric.gauge.data_points or metric.sum.data_points
                  or metric.histogram.data_points)
        for point in points:
            entry = {"kind": kind, "time_unix_nano": point.time_unix_nano,
                     "slot": point.attributes[0].value.string_value}
            if kind in ("gauge_int", "sum_int"):
                entry["value_int"] = point.as_int
            elif kind in ("gauge_double", "sum_double"):
                entry["value_double"] = point.as_double
            else:
                entry.update(count=point.count, sum=point.sum,
                             bucket_counts=list(point.bucket_counts),
                             explicit_bounds=list(point.explicit_bounds))
            seq = (point.time_unix_nano - measurement.LOG_BASE_TIME_NS) // \
                measurement.METRIC_TIME_STEP_NS
            records[seq] = entry
    return records


def compare_sample(db, root, generator, acked, signal, *, count=SAMPLE_RECORDS, seed=0):
    """Stored rows of randomly chosen acknowledged records against the
    generator, field by field, with the series attributes of their latest
    descriptor."""
    if not acked:
        return {"sampled_count": 0, "mismatches": []}
    rpr = generator.workload.records_per_request
    chooser = random.Random(seed)
    wanted = sorted({chooser.choice(acked) * rpr + chooser.randrange(rpr) for _ in range(count)})
    values, canonical = measurement._duck_latest_descriptor(root, signal)
    times = ",".join(str(time_of(seq)) for seq in wanted)
    if signal == "logs":
        columns = ("v.time_unix_nano, v.body, "
                   "list_extract(map_extract(s.attrs, 'logger.name'), 1)")
    else:
        columns = ("v.time_unix_nano, replace(v.metric_name, "
                   f"'{measurement.METRIC_NAME_PREFIX}', ''), v.value_int, v.value_double, "
                   "v.count, v.sum, v.bucket_counts, v.explicit_bounds, "
                   "list_extract(map_extract(s.attrs, 'series.slot'), 1)")
    rows = db.execute(
        f"SELECT {columns} FROM {values} v JOIN {canonical} s ON v.series_id = s.series_id "
        f"WHERE v.time_unix_nano IN ({times})"
    ).fetchall()
    stored = {}
    for row in rows:
        stored.setdefault((int(row[0]) - measurement.LOG_BASE_TIME_NS)
                          // measurement.METRIC_TIME_STEP_NS, []).append(row)
    mismatches = []
    cache = {}
    for seq in wanted:
        index = seq // rpr
        if index not in cache:
            cache[index] = expected_records(generator, index)
        expected = cache[index][seq]
        found = stored.get(seq, [])
        if len(found) != 1:
            mismatches.append({"seq": seq, "reason": f"{len(found)} stored rows"})
            continue
        row = found[0]
        if signal == "logs":
            actual = {"time_unix_nano": int(row[0]), "body": row[1], "logger": row[2]}
        else:
            actual = {"kind": row[1], "time_unix_nano": int(row[0]), "slot": row[8]}
            if expected["kind"] in ("gauge_int", "sum_int"):
                actual["value_int"] = row[2]
            elif expected["kind"] in ("gauge_double", "sum_double"):
                actual["value_double"] = row[3]
            else:
                actual.update(count=row[4], sum=row[5], bucket_counts=list(row[6] or []),
                              explicit_bounds=list(row[7] or []))
        if actual != expected:
            differing = sorted(key for key in expected if actual.get(key) != expected[key])
            mismatches.append({"seq": seq, "fields": differing})
    return {"sampled_count": len(wanted), "mismatches": mismatches[:20],
            "mismatch_count": len(mismatches)}


def file_invariants(db, root) -> dict:
    """Every part file against its own metadata, in SQL.

    The layout and the recorded row count of each file; the declared sort
    order of each values file, read in file order; each descriptor's
    identity hash; and the rule that every values row of a partition and
    writer is covered by a descriptor of the same signal, partition and
    writer.
    """
    import xxhash
    from pathlib import Path

    files = sorted(Path(root).rglob("*.parquet"))
    problems = []
    case = measurement._SilentAsserts()
    descriptor_rows = 0
    for path in files:
        metadata = dict(db.execute(
            "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)", [str(path)]
        ).fetchall())
        try:
            test_e2e.verify_layout(case, root, path, metadata)
        except AssertionError as error:
            problems.append(str(error)[:300])
            continue
        rows = db.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
        if int(metadata["row_count"]) != rows:
            problems.append(f"{path.name}: {rows} rows, metadata says {metadata['row_count']}")
        if "dataset=series" in path.parts:
            for series_id, identity in db.execute(
                "SELECT series_id, identity_bytes FROM read_parquet(?)", [str(path)]
            ).fetchall():
                descriptor_rows += 1
                if xxhash.xxh3_128_digest(identity) != series_id:
                    problems.append(f"{path.name}: identity hash mismatch")
                    break
        elif metadata.get("sort_key") != "none":
            unsorted = db.execute(
                "SELECT count(*) FROM (SELECT series_id, time_unix_nano, "
                "lag(series_id) OVER w AS ps, lag(time_unix_nano) OVER w AS pt "
                "FROM read_parquet(?, file_row_number=true) "
                "WINDOW w AS (ORDER BY file_row_number)) "
                "WHERE ps > series_id OR (ps = series_id AND pt > time_unix_nano)",
                [str(path)],
            ).fetchone()[0]
            if unsorted:
                problems.append(f"{path.name}: {unsorted} rows out of sort order")
    for signal in ("logs", "metrics"):
        values = root / f"v=1/signal={signal}/dataset=values"
        series = root / f"v=1/signal={signal}/dataset=series"
        if not values.is_dir() or not series.is_dir():
            continue
        partition = ("date, hour, regexp_extract(filename, "
                     "'-([0-9a-f]{32})-[0-9]+\\.parquet$', 1) AS boot")
        uncovered = db.execute(
            f"WITH v AS (SELECT DISTINCT series_id, {partition} FROM read_parquet("
            f"{test_e2e.sql_string(values / '**/*.parquet')}, filename=true, "
            "hive_partitioning=true)), "
            f"s AS (SELECT DISTINCT series_id, {partition} FROM read_parquet("
            f"{test_e2e.sql_string(series / '**/*.parquet')}, filename=true, "
            "hive_partitioning=true)) "
            "SELECT count(*) FROM v WHERE NOT EXISTS (SELECT 1 FROM s WHERE "
            "s.series_id = v.series_id AND s.date = v.date AND s.hour = v.hour "
            "AND s.boot = v.boot)"
        ).fetchone()[0]
        if uncovered:
            problems.append(f"{signal}: {uncovered} values series without a descriptor")
    return {"part_file_count": len(files), "descriptor_rows_count": descriptor_rows,
            "problems": problems}


def aggregate_oracle(root, generator, acked, failed=(), *, sample=SAMPLE_RECORDS,
                     cross_reader=True) -> dict:
    """Check every stored row against the acknowledged requests.

    Per signal: coverage per request (no acknowledged record lost, none
    duplicated, none foreign), the global count and sequence sum, a random
    sample compared field by field with the generator, and the file
    invariants; with `cross_reader`, clickhouse-local's count and sequence
    sum must equal DuckDB's.
    """
    import duckdb
    from pathlib import Path

    root = Path(root).resolve()
    workload = generator.workload
    rpr = workload.records_per_request
    report = {"root": str(root), "generator": generator.as_json(), "signals": {}}
    problems = []
    with duckdb.connect() as db:
        invariants = file_invariants(db, root)
        report["files"] = invariants
        problems += invariants["problems"]
        try:
            measurement._check_list_shapes(measurement._SilentAsserts(), db, root, workload)
        except AssertionError as error:
            problems.append(f"list shapes: {error}"[:300])
        report["series_cardinality"] = measurement._series_cardinality(db, root)
        for signal in ("logs", "metrics"):
            acked_signal = sorted(r for r in acked if workload.signal_of(r) == signal)
            failed_signal = sorted(r for r in failed if workload.signal_of(r) == signal)
            glob = root / f"v=1/signal={signal}/dataset=values/**/*.parquet"
            if not any(root.glob(f"v=1/signal={signal}/dataset=values/**/*.parquet")):
                if acked_signal:
                    problems.append(f"{signal}: {len(acked_signal)} acknowledged requests, "
                                    f"no stored file")
                continue
            relation = (f"(SELECT {_seq('time_unix_nano')} AS seq FROM read_parquet("
                        f"{test_e2e.sql_string(glob)}, hive_partitioning=false))")
            coverage = request_coverage(db, relation, rpr, acked_signal, failed_signal)
            coverage["sample"] = compare_sample(db, root, generator, acked_signal, signal,
                                                count=sample)
            report["signals"][signal] = coverage
            for key in ("missing_record_count", "duplicate_record_count",
                        "unexpected_record_count", "out_of_range_positions_count"):
                if coverage[key]:
                    problems.append(f"{signal}: {key}={coverage[key]}")
            if coverage["sample"].get("mismatch_count"):
                problems.append(f"{signal}: {coverage['sample']['mismatch_count']} sampled "
                                f"records differ: {coverage['sample']['mismatches'][:3]}")
    if cross_reader:
        report["readers"] = clickhouse_agreement(root, report["signals"])
        problems += report["readers"].get("problems", [])
    report["problems"] = problems
    report["passed"] = not problems
    return report


def clickhouse_agreement(root, signals) -> dict:
    """clickhouse-local's count and sequence sum of each signal against DuckDB's."""
    agreement = {"problems": []}
    with test_e2e.clickhouse_reader(root) as clickhouse:
        for signal, coverage in signals.items():
            glob = f"v=1/signal={signal}/dataset=values/**/*.parquet"
            count, total = clickhouse(
                f"SELECT count(), sum(intDiv(toInt128(time_unix_nano) - "
                f"{measurement.LOG_BASE_TIME_NS}, {measurement.METRIC_TIME_STEP_NS})) "
                f"FROM file({test_e2e.sql_string(glob)}, 'Parquet')"
            )[0][:2]
            agreement[signal] = {"count": int(count), "seq_sum": int(total)}
            if int(count) != coverage["stored_record_count"] or int(total) != coverage["seq_sum"]:
                agreement["problems"].append(
                    f"reader disagreement for {signal}: clickhouse count {count} sum {total}, "
                    f"duckdb count {coverage['stored_record_count']} sum {coverage['seq_sum']}"
                )
    return agreement
