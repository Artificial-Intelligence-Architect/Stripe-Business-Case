# Monitoring Stack

Complete observability for Stripe data infrastructure.

## Components
- **Prometheus:** Metrics collection (1B+ time-series/day)
- **Grafana:** Dashboards & visualization
- **ELK Stack:** Logs (Elasticsearch + Kibana)
- **Jaeger:** Distributed tracing

## Alert Rules
See `prometheus/alert_rules.yml` for all alert definitions.

## Dashboards
See docs/09_observability.md for dashboard architecture.

## Deploy
```bash
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace
```

See docs/09_observability.md for full configuration.
