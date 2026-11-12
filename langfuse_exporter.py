"""Langfuse -> Prometheus exporter (stdlib only).

Langfuse v4 has no /metrics; its public metrics API (GET /api/public/v2/metrics) answers aggregate
queries over ClickHouse. This exporter asks it for every measure of every view on a timer
(LANGFUSE_EXPORTER_INTERVAL s, queries in parallel) and serves the last result at /metrics.

Coverage (Langfuse 4.78; the API lists them in its 400 answers):
  observations: all 21 measures. Additive ones as all-time counters by type/name/model/level/
    environment (rate() and increase() work); uniq ones as gauges; latency-like and per-request
    ones as window distributions (avg/min/max/p50/p75/p90/p95/p99 over 15m/1h/24h) and 24 h
    histograms; usageByType/costByType by usage/cost type; toolCallInvocations by tool;
    countScores on its own (it empties any query it shares).
  dimensions: every low-cardinality one as a breakdown (count, tokens); the high-cardinality ones
    (sessions, users, experiments, gateway keys, evaluators) as a top-N; row ids feed the uniqs.
  scores-numeric / -boolean / -categorical: counts by name/source/data type/environment, numeric
    and boolean value statistics, boolean and categorical values.

Quirks: a `type` filter or toolCallInvocations repeat rows per called tool (so: group by type);
the default row limit is 100 (so: ask for 1000, count truncated answers).

Env: LANGFUSE_URL (http://langfuse-web:3000), LANGFUSE_PUBLIC_KEY, LANGFUSE_SECRET_KEY,
LANGFUSE_EXPORTER_PORT (9877), LANGFUSE_EXPORTER_INTERVAL (60), LANGFUSE_CA_FILE (optional).
"""

import base64
import json
import os
import re
import ssl
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

URL = os.environ.get("LANGFUSE_URL", "http://langfuse-web:3000").rstrip("/")
_PAIR = f"{os.environ['LANGFUSE_PUBLIC_KEY']}:{os.environ['LANGFUSE_SECRET_KEY']}"
AUTH = "Basic " + base64.b64encode(_PAIR.encode()).decode()
PORT = int(os.environ.get("LANGFUSE_EXPORTER_PORT", "9877"))
INTERVAL = int(os.environ.get("LANGFUSE_EXPORTER_INTERVAL", "60"))
CTX = ssl.create_default_context(cafile=os.environ.get("LANGFUSE_CA_FILE") or None)
EPOCH = "2020-01-01T00:00:00Z"
ROW_LIMIT = 1000
TOP_N = 25
WINDOWS = {"15m": timedelta(minutes=15), "1h": timedelta(hours=1), "24h": timedelta(hours=24)}
STATS = ("avg", "min", "max", "p50", "p75", "p90", "p95", "p99")
MS = {"latency", "streamingLatency", "timeToFirstToken"}  # milliseconds in the API -> seconds

# fmt: off
# additive observation measures: all-time counters (metric suffix, measure)
SUMS = [
    ("input_tokens", "inputTokens"), ("output_tokens", "outputTokens"),
    ("total_tokens", "totalTokens"), ("input_cost_usd", "inputCost"),
    ("output_cost_usd", "outputCost"), ("total_cost_usd", "totalCost"),
    ("tool_calls", "toolCalls"), ("tool_definitions", "toolDefinitions"),
    ("latency_seconds", "latency"), ("streaming_latency_seconds", "streamingLatency"),
    ("time_to_first_token_seconds", "timeToFirstToken"),
]
# distinct counts: gauges (metric suffix, measure)
UNIQS = [("traces", "traceId"), ("users", "uniqueUserIds"), ("sessions", "uniqueSessionIds")]
# per-request measures: window distributions and histograms
DISTRIBUTIONS = [
    "latency", "streamingLatency", "timeToFirstToken", "tokensPerSecond", "outputTokensPerSecond",
    "inputTokens", "outputTokens", "totalTokens", "totalCost", "toolCalls", "toolDefinitions",
]
GROUP = ["type", "name", "providedModelName", "level", "environment"]
LOW_CARD = [
    "traceName", "promptName", "promptVersion", "release", "version", "traceRelease",
    "traceVersion", "tags", "isRootObservation", "isEvaluatorTest", "startTimeMonth", "toolNames",
    "skillNamesAvailable", "skillsNamesLoaded", "skillIdsAvailable", "skillIdsLoaded",
    "loadedFilePaths",
]
HIGH_CARD = [
    "sessionId", "userId", "experimentName", "experimentId", "experimentDatasetId",
    "aiGatewayKeyId", "aiGatewayKeyOwnerUserId", "evaluatorId",
]
# fmt: on
SCORE_VIEWS = ("scores-numeric", "scores-boolean", "scores-categorical")
SCORE_GROUP = ["name", "source", "dataType", "environment"]

state = {"text": "", "errors": 0, "truncated": 0, "ok": 0.0}


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def get(path: str, timeout: int = 60) -> dict:
    req = urllib.request.Request(URL + path, headers={"Authorization": AUTH})
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return json.loads(r.read())


def snake(s: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", s).lower()


def esc(v) -> str:
    if isinstance(v, list):
        v = ",".join(map(str, v))
    s = "none" if v is None or v == "" else str(v).lower() if isinstance(v, bool) else str(v)
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")


def num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


class Out:
    def __init__(self):
        self.series: dict[str, list[str]] = {}
        self.meta: dict[str, tuple[str, str]] = {}
        self.lock = threading.Lock()

    def add(self, _metric, _value, _kind="gauge", _help="", **labels):
        metric, kind, help_ = _metric, _kind, _help
        value = num(_value)
        if value is None:
            return
        lab = ",".join(f'{k}="{esc(v)}"' for k, v in labels.items())
        with self.lock:
            self.meta.setdefault(metric, (kind, help_ or metric))
            self.series.setdefault(metric, []).append(f"{metric}{{{lab}}} {value:.10g}")

    def render(self) -> str:
        lines = []
        for metric, rows in self.series.items():
            kind, text = self.meta[metric]
            lines += [f"# HELP {metric} {text}", f"# TYPE {metric} {kind}", *rows]
        return "\n".join(lines) + "\n"


class Run:
    """One refresh: queries, counted errors and truncations."""

    def __init__(self, out: Out):
        self.out, self.errors, self.truncated, self.queries = out, 0, 0, 0
        self.lock = threading.Lock()

    def query(self, view, metrics, dims=(), start=EPOCH, order=None, limit=ROW_LIMIT):
        q = {
            "view": view,
            "metrics": [{"measure": m, "aggregation": a} for m, a in metrics],
            "dimensions": [{"field": d} for d in dims],
            "filters": [],
            "fromTimestamp": start,
            "toTimestamp": iso(datetime.now(UTC) + timedelta(minutes=5)),
            "config": {"row_limit": limit},
        }
        if order:
            q["orderBy"] = [{"field": order, "direction": "desc"}]
        with self.lock:
            self.queries += 1
        rows = get("/api/public/v2/metrics?query=" + urllib.parse.quote(json.dumps(q)))["data"]
        if len(rows) >= limit and not order:
            with self.lock:
                self.truncated += 1
        return rows


def labels(r, dims):
    names = {"providedModelName": "model", "dataType": "data_type"}
    return {names.get(d, snake(d)): r.get(d) for d in dims}


def jobs(run: Run):
    out, now = run.out, datetime.now(UTC)

    def health():
        t0 = time.time()
        try:
            ok = get("/api/public/health", timeout=10).get("status") == "OK"
        except Exception:
            ok = False
        out.add("langfuse_up", int(ok), _help="1 when /api/public/health answers OK")
        out.add("langfuse_health_seconds", time.time() - t0, _help="Health request duration")

    def totals():
        m = [("count", "count")] + [(x, "sum") for _, x in SUMS] + [(x, "uniq") for _, x in UNIQS]
        for r in run.query("observations", m, GROUP):
            lab = labels(r, GROUP)
            out.add(
                "langfuse_observations_total",
                r.get("count_count"),
                "counter",
                "Observations (all time)",
                **lab,
            )
            for suffix, x in SUMS:
                v = num(r.get(f"sum_{x}")) or 0.0  # null = nothing recorded yet
                v = v / 1000 if x in MS else v
                out.add(
                    f"langfuse_observation_{suffix}_total",
                    v,
                    "counter",
                    f"Sum of {x} (all time)",
                    **lab,
                )
            for suffix, x in UNIQS:
                out.add(
                    f"langfuse_observation_{suffix}",
                    r.get(f"uniq_{x}"),
                    _help=f"Distinct {x} (all time)",
                    **lab,
                )

    def scores_on_observations():
        dims = ["type", "providedModelName"]
        for r in run.query("observations", [("countScores", "sum")], dims):
            out.add(
                "langfuse_observation_scores_total",
                r.get("sum_countScores"),
                "counter",
                "Scores attached to observations",
                **labels(r, dims),
            )

    def by_usage_type():
        for r in run.query("observations", [("usageByType", "sum")], ["usageType"]):
            out.add(
                "langfuse_usage_by_type_total",
                r.get("sum_usageByType"),
                "counter",
                "Usage units by usage type (input, cached, output, reasoning ...)",
                usage_type=r.get("usageType"),
            )

    def by_cost_type():
        for r in run.query("observations", [("costByType", "sum")], ["costType"]):
            out.add(
                "langfuse_cost_by_type_usd_total",
                r.get("sum_costByType"),
                "counter",
                "Cost in USD by cost type",
                cost_type=r.get("costType"),
            )

    def by_called_tool():
        m = [("count", "count"), ("toolCallInvocations", "sum")]
        for r in run.query("observations", m, ["calledToolNames"]):
            if r.get("calledToolNames"):
                tool = r["calledToolNames"]
                out.add(
                    "langfuse_generations_by_tool_total",
                    r.get("count_count"),
                    "counter",
                    "Generations that called the tool",
                    tool=tool,
                )
                out.add(
                    "langfuse_tool_invocations_total",
                    r.get("sum_toolCallInvocations"),
                    "counter",
                    "Tool calls by tool",
                    tool=tool,
                )

    def breakdown(dim):
        def fn():
            m = [("count", "count"), ("totalTokens", "sum"), ("latency", "sum")]
            for r in run.query("observations", m, [dim]):
                lab = {"dimension": dim, "value": r.get(dim)}
                out.add(
                    "langfuse_breakdown_observations_total",
                    r.get("count_count"),
                    "counter",
                    "Observations by dimension value",
                    **lab,
                )
                out.add(
                    "langfuse_breakdown_tokens_total",
                    r.get("sum_totalTokens"),
                    "counter",
                    "Tokens by dimension value",
                    **lab,
                )
                lat = num(r.get("sum_latency"))
                out.add(
                    "langfuse_breakdown_latency_seconds_total",
                    None if lat is None else lat / 1000,
                    "counter",
                    "Latency sum by dimension value",
                    **lab,
                )

        fn.__name__ = f"breakdown_{dim}"
        return fn

    def top(dim):
        def fn():
            m = [
                ("count", "count"),
                ("totalTokens", "sum"),
                ("latency", "sum"),
                ("toolCalls", "sum"),
                ("totalCost", "sum"),
            ]
            rows = run.query("observations", m, [dim], order="count_count", limit=TOP_N)
            for r in rows:
                lab = {"dimension": dim, "value": r.get(dim)}
                out.add(
                    "langfuse_top_observations",
                    r.get("count_count"),
                    _help=f"Top {TOP_N} by observations (all time)",
                    **lab,
                )
                out.add("langfuse_top_tokens", r.get("sum_totalTokens"), **lab)
                lat = num(r.get("sum_latency"))
                out.add("langfuse_top_latency_seconds", None if lat is None else lat / 1000, **lab)
                out.add("langfuse_top_tool_calls", r.get("sum_toolCalls"), **lab)
                out.add("langfuse_top_cost_usd", r.get("sum_totalCost"), **lab)

        fn.__name__ = f"top_{dim}"
        return fn

    def window_counts(w, span):
        def fn():
            dims = ["type", "providedModelName"]
            m = [("count", "count")] + [(x, "uniq") for _, x in UNIQS]
            m += [
                (x, "sum")
                for x in ("inputTokens", "outputTokens", "totalTokens", "totalCost", "toolCalls")
            ]
            for r in run.query("observations", m, dims, iso(now - span)):
                lab = {"window": w, **labels(r, dims)}
                out.add(
                    "langfuse_window_observations",
                    r.get("count_count"),
                    _help="Observations in the window",
                    **lab,
                )
                for suffix, x in UNIQS:
                    out.add(f"langfuse_window_{suffix}", r.get(f"uniq_{x}"), **lab)
                for x in ("inputTokens", "outputTokens", "totalTokens", "totalCost", "toolCalls"):
                    out.add(f"langfuse_window_{snake(x)}_sum", r.get(f"sum_{x}"), **lab)

        fn.__name__ = f"window_counts_{w}"
        return fn

    def window_stats(w, span, measure):
        def fn():
            dims = ["type", "providedModelName"]
            for r in run.query(
                "observations", [(measure, s) for s in STATS], dims, iso(now - span)
            ):
                for s in STATS:
                    v = num(r.get(f"{s}_{measure}"))
                    v = v / 1000 if v is not None and measure in MS else v
                    out.add(
                        "langfuse_window_stat",
                        v,
                        _help="Per-request distribution in the window (ms measures in s)",
                        window=w,
                        measure=measure,
                        stat=s,
                        **labels(r, dims),
                    )

        fn.__name__ = f"window_{w}_{measure}"
        return fn

    def histogram(measure):
        def fn():
            for r in run.query(
                "observations", [(measure, "histogram")], ["type"], iso(now - timedelta(hours=24))
            ):
                f = 1000 if measure in MS else 1
                for lo, hi, c in r.get(f"histogram_{measure}") or []:
                    out.add(
                        "langfuse_histogram_24h",
                        round(c),
                        _help="Observations per value band, last 24 h (ms measures in s)",
                        measure=measure,
                        type=r.get("type"),
                        lower=f"{lo / f:.6g}",
                        upper=f"{hi / f:.6g}",
                    )

        fn.__name__ = f"histogram_{measure}"
        return fn

    def scores(view):
        def fn():
            kind = view.split("-")[1]
            m = [("count", "count")]
            if kind != "categorical":
                m += [("value", s) for s in ("avg", "min", "max", "sum", "p50", "p95")]
            for r in run.query(view, m, SCORE_GROUP):
                lab = {"kind": kind, **labels(r, SCORE_GROUP)}
                out.add(
                    "langfuse_scores_total",
                    r.get("count_count"),
                    "counter",
                    "Scores (all time)",
                    **lab,
                )
                for s in ("avg", "min", "max", "sum", "p50", "p95"):
                    out.add(
                        "langfuse_score_value",
                        r.get(f"{s}_value"),
                        _help="Score value statistics (all time)",
                        stat=s,
                        **lab,
                    )
            value_dim = {"boolean": "booleanValue", "categorical": "stringValue"}.get(kind)
            if value_dim:
                for r in run.query(view, [("count", "count")], ["name", value_dim]):
                    out.add(
                        "langfuse_score_values_total",
                        r.get("count_count"),
                        "counter",
                        "Scores by value",
                        kind=kind,
                        name=r.get("name"),
                        value=r.get(value_dim),
                    )

        fn.__name__ = f"scores_{view}"
        return fn

    yield from (health, totals, scores_on_observations, by_usage_type, by_cost_type, by_called_tool)
    yield from (breakdown(d) for d in LOW_CARD)
    yield from (top(d) for d in HIGH_CARD)
    for w, span in WINDOWS.items():
        yield window_counts(w, span)
        yield from (window_stats(w, span, m) for m in DISTRIBUTIONS)
    yield from (histogram(m) for m in DISTRIBUTIONS)
    yield from (scores(v) for v in SCORE_VIEWS)


def refresh() -> None:
    out = Out()
    run, t0 = Run(out), time.time()

    def safe(fn):
        try:
            fn()
        except Exception as e:  # one failed query must not drop the other series
            with run.lock:
                run.errors += 1
            print(f"query failed: {fn.__name__}: {e}", flush=True)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(safe, jobs(run)))

    state["errors"] += run.errors
    state["truncated"] += run.truncated
    if run.errors == 0:
        state["ok"] = time.time()
    out.add("langfuse_exporter_refresh_seconds", time.time() - t0, _help="Last refresh duration")
    out.add("langfuse_exporter_queries", run.queries, _help="Metrics API queries per refresh")
    out.add("langfuse_exporter_query_errors", run.errors, _help="Failed queries, last refresh")
    out.add("langfuse_exporter_query_errors_total", state["errors"], "counter", "Failed queries")
    out.add(
        "langfuse_exporter_truncated_total",
        state["truncated"],
        "counter",
        f"Answers that hit the {ROW_LIMIT} row limit",
    )
    out.add(
        "langfuse_exporter_last_success_timestamp_seconds",
        state["ok"],
        _help="End of the last refresh without errors",
    )
    state["text"] = out.render()


def loop() -> None:
    while True:
        try:
            refresh()
        except Exception as e:
            print(f"refresh failed: {e}", flush=True)
        time.sleep(INTERVAL)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0] not in ("/metrics", "/"):
            self.send_error(404)
            return
        body = state["text"].encode()
        self.send_response(200 if body else 503)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # no access log
        return


if __name__ == "__main__":
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
