# Pipeline monitoring

`cloudwatch_dashboard.json` defines a CloudWatch dashboard for the pipeline's custom metrics
(namespace `EcommerceDataPlatform/dev`, region `us-east-1`).

Widgets:
- **Pipeline Success Rate (7 days):** average of `PipelineRunSuccess` (1 per successful run, 0 per failed run).
- **Rows Processed per Run:** `RowsExtracted`, `RowsTransformed`, `RowsLoaded`.
- **Data Freshness Lag (minutes):** `DataFreshnessLagMinutes`.
- **Quarantine Rate (%):** `RowsQuarantined / (RowsLoaded + RowsQuarantined) * 100`, computed with a metric expression.
- **Airflow Task Duration:** placeholder. Airflow does not publish to CloudWatch; it emits its own metrics via statsd, which a StatsD or Prometheus exporter scrapes. Dashboard it there.

## Deploy

```bash
aws cloudwatch put-dashboard \
  --dashboard-name EcommerceDataPlatform-dev \
  --dashboard-body file://monitoring/cloudwatch_dashboard.json \
  --region us-east-1
```

Against fakecloud (local emulator), add `--endpoint-url http://localhost:4566`.

## Notes

- Metrics are published without dimensions, so all pipelines share one series per metric name. Per-pipeline splitting would need a `PipelineName` dimension on both the publisher and the widgets.
- Metrics are only published when `ENVIRONMENT` is `dev`, `staging` or `prod` and `AWS_REGION` is set.
