# langfuse-prometheus-exporter

Prometheus metrics for a self-hosted **Langfuse v4**, from its public metrics API.

Langfuse has no `/metrics` endpoint. Its public metrics API (`GET /api/public/v2/metrics`)
answers aggregate queries over ClickHouse. This exporter asks it for every measure of every view
on a timer (82 queries per refresh, under a second on a small instance) and serves the last
result at `/metrics`, ready for Prometheus, VictoriaMetrics or any OpenMetrics scraper.

- Single file, Python standard library only (no pip install).
- All 21 observation measures, every dimension, the three score views.
- Counters for all-time totals (so `rate()` / `increase()` work), gauges for rolling windows
  (15m / 1h / 24h), 24 h histograms.
- Works around the API quirks listed below, so the numbers match the Langfuse UI.

Tested against Langfuse 4.78 (self-hosted, v4 `events_only` mode). The v1 endpoints
(`/api/public/metrics`, `/metrics/daily`) are not available there; this uses v2 only.

## Run

```sh
docker build -t langfuse-prometheus-exporter .
docker run -d --name langfuse-exporter -p 9877:9877 \
  -e LANGFUSE_URL=http://langfuse-web:3000 \
  -e LANGFUSE_PUBLIC_KEY=pk-lf-... \
  -e LANGFUSE_SECRET_KEY=sk-lf-... \
  langfuse-prometheus-exporter
curl -s localhost:9877/metrics | head
```

Or without a container (Python 3.11+):

```sh
LANGFUSE_URL=https://langfuse.example.com LANGFUSE_PUBLIC_KEY=pk-lf-... \
LANGFUSE_SECRET_KEY=sk-lf-... python3 langfuse_exporter.py
```

The key pair is a **project** API key (Langfuse > Settings > API Keys); the metrics are for that
project. Run one exporter per project.

| Variable | Default | |
|---|---|---|
| `LANGFUSE_URL` | `http://langfuse-web:3000` | Langfuse web (inside the compose network, or the public URL) |
| `LANGFUSE_PUBLIC_KEY` | – (required) | `pk-lf-…` |
| `LANGFUSE_SECRET_KEY` | – (required) | `sk-lf-…` |
| `LANGFUSE_EXPORTER_PORT` | `9877` | Listen port |
| `LANGFUSE_EXPORTER_INTERVAL` | `60` | Seconds between refreshes |
| `LANGFUSE_CA_FILE` | system CAs | CA bundle for an `https` URL with a private CA |

The exporter refreshes in the background; a scrape always returns the last complete result
(HTTP 503 until the first refresh finishes). Scraping more often than the interval gains nothing.

### Prometheus

```yaml
scrape_configs:
  - job_name: langfuse
    scrape_interval: 60s
    scrape_timeout: 50s
    static_configs: [{targets: ["langfuse-exporter:9877"]}]
```

VictoriaMetrics single-node can scrape it itself with the same block in `-promscrape.config`.

The metrics contain usage data (session ids, tool names, trace names), not prompts or answers.
Still, keep the port inside your network or behind a proxy that only lets the scraper in.

## Metrics

46 metric names. Latency-like measures are in seconds (the API returns milliseconds).
Labels: `type` (GENERATION, SPAN, …), `name`, `model`, `level`, `environment` unless noted.

| Metric | Labels | Meaning |
|---|---|---|
| `langfuse_up`, `langfuse_health_seconds` | – | `/api/public/health` OK, and its duration |
| `langfuse_observations_total` | type, name, model, level, environment | Observations (all time) |
| `langfuse_observation_{input,output,total}_tokens_total` | same | Tokens |
| `langfuse_observation_{input,output,total}_cost_usd_total` | same | Cost by the model price table |
| `langfuse_observation_tool_calls_total`, `…_tool_definitions_total` | same | Tool calls emitted, tools defined |
| `langfuse_observation_{latency,streaming_latency,time_to_first_token}_seconds_total` | same | Latency sums (mean = Δsum / Δcount) |
| `langfuse_observation_{traces,users,sessions}` | same | Distinct counts (gauges) |
| `langfuse_observation_scores_total` | type, model | Scores attached to observations |
| `langfuse_usage_by_type_total` | usage_type | input, input_cached_tokens, output, output_reasoning_tokens, total … |
| `langfuse_cost_by_type_usd_total` | cost_type | Cost by type |
| `langfuse_generations_by_tool_total`, `langfuse_tool_invocations_total` | tool | Generations that called a tool, tool calls that ran |
| `langfuse_breakdown_{observations,tokens,latency_seconds}_total` | dimension, value | Every low-cardinality dimension: traceName, prompt name/version, release, version, tags, root/child, evaluator test, month, toolNames, skills, loaded files |
| `langfuse_top_{observations,tokens,latency_seconds,tool_calls,cost_usd}` | dimension, value | Top 25 of sessionId, userId, experiments, AI gateway keys, evaluators |
| `langfuse_window_{observations,traces,users,sessions}` | window, type, model | Counts in 15m / 1h / 24h |
| `langfuse_window_{input,output,total}_tokens_sum`, `…_total_cost_sum`, `…_tool_calls_sum` | window, type, model | Sums in the window |
| `langfuse_window_stat` | window, type, model, measure, stat | avg / min / max / p50 / p75 / p90 / p95 / p99 of latency, streamingLatency, timeToFirstToken, tokensPerSecond, outputTokensPerSecond, input/output/total tokens, totalCost, toolCalls, toolDefinitions |
| `langfuse_histogram_24h` | measure, type, lower, upper | 24 h histograms of the same measures |
| `langfuse_scores_total`, `langfuse_score_value`, `langfuse_score_values_total` | kind, name, source, data_type, environment, stat, value | scores-numeric / -boolean / -categorical |
| `langfuse_exporter_*` | – | Refresh time, queries per refresh, errors, truncated answers, last good refresh |

Measures Langfuse has no value for yet (for example time to first token when nothing streams,
or scores when none exist) produce no series until data arrives.

### Example queries

```promql
# generations per minute, by model
sum by (model) (rate(langfuse_observations_total{type="GENERATION"}[5m])) * 60
# p95 latency over the last hour
max by (model) (langfuse_window_stat{type="GENERATION",window="1h",measure="latency",stat="p95"})
# prompt cache reuse
sum(langfuse_usage_by_type_total{usage_type="input_cached_tokens"})
  / (sum(langfuse_usage_by_type_total{usage_type="input_cached_tokens"}) + sum(langfuse_usage_by_type_total{usage_type="input"}))
# error rate
sum(langfuse_observations_total{type="GENERATION",level="ERROR"}) / sum(langfuse_observations_total{type="GENERATION"})
```

## Grafana dashboard

[](grafana/langfuse-dashboard.json): "Langfuse Neon", 73 panels
over every metric above (pulse stats, traffic, latency, tokens and speed, tools, sessions,
breakdowns, cost and scores, pipeline health), neon palette. Import it in Grafana (Dashboards >
New > Import) and pick your Prometheus-type data source in the **Data source** variable; the
**Model** and **Window** variables filter the panels.

- The "Pipeline health" panels expect the scrape job to be named .
- The four histogram panels sort their bands with  (MetricsQL), so they
  need VictoriaMetrics. On Prometheus, use  with
  , or drop the function (bands then show
  unsorted). Everything else is plain PromQL.

## API quirks it works around

Found while checking every number against the Langfuse UI:

- A `type` filter, or the `toolCallInvocations` measure, makes Langfuse join the called tool
  names and repeat every row once per tool, so counts come out about 10× too high. Generations
  are grouped by `type` and never filtered; `toolCallInvocations` is only asked for per tool.
- `countScores` in a query with other measures empties the whole answer. It has its own query.
- The default `config.row_limit` is 100 and cuts silently. Every query asks for 1000, and
  `langfuse_exporter_truncated_total` counts answers that hit it.
- High-cardinality dimensions (sessionId, userId, experiments, gateway keys) need `orderBy`
  desc plus `row_limit`, so they are top-N only.

The API lists its own measures and dimensions in its HTTP 400 answer (send an unknown one), which
is how the coverage was checked: 21 observation measures and 36 observation dimensions in 4.78.

## License

MIT
