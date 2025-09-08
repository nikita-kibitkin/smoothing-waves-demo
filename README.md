[![Java CI with Maven](https://github.com/nikita-kibitkin/two-queue-priority-demo/actions/workflows/maven.yml/badge.svg)](https://github.com/nikita-kibitkin/two-queue-priority-demo/actions/workflows/maven.yml)

# Two-Queue Priority Demo (Spring Boot + Kafka + Prometheus + Grafana)

In this demo, you can see how adding a second higher priority queue significantly reduces the p99 latency of priority operations, while barely increasing p99 latency for normal priority operations.

In the original production business case, we significantly sped up customer live payments (higher priority) by increasing their priority compared to recurring subscriptions (normal priority).

## Prerequisites
* Java 17
* Maven 3.9+
* Docker / Docker Compose v2

## Quick Start
```bash
git clone https://github.com/nikita-kibitkin/two-queue-priority-demo.git
cd two-queue-priority-demo
chmod +x scripts/run_with_warmup.sh
./scripts/run_with_warmup.sh
```
Available in a couple of minutes:
* **Prometheus:** <http://localhost:9090>
* **Grafana:** <http://localhost:3000> (login admin/admin)
*  `grafana/priority_dashboard.json` → dashboard with P50/P95/P99.

## Configuration
application.yml contains `duration-minutes`, `high-priority-chance`(0.5=50% chance. 0=high queue only), `lambda` (messages per second) parameters. Change and run `run_with_warmup.sh`.
