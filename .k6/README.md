# Email reservation load tests

Two k6 scripts exercise the implemented email-proof API:

- `email-reservations.js` sends `POST /email-reservations` with a unique email
  for each iteration and expects `202`, a canonical email, and `account_id`.
- `request-verification-code.js` reserves one email in `setup()` and repeatedly
  sends `POST /email-reservations/{email}/verification-codes` for it. Concurrent
  requests may receive `202` or `409` after bounded optimistic retries.

Each accepted request appends an event and a code-delivery outbox job. Run
these scripts against an isolated environment; they grow the database and
queue. They do not test account creation or prove mailbox delivery.

Install [k6](https://grafana.com/docs/k6/latest/), start the service and its
PostgreSQL database, then run from the repository root:

```sh
mkdir -p .k6/results
K6_WEB_DASHBOARD=true \
K6_WEB_DASHBOARD_PORT=-1 \
K6_WEB_DASHBOARD_PERIOD=1s \
K6_WEB_DASHBOARD_EXPORT=.k6/results/email-reservations-dashboard.html \
k6 run -e BASE_URL=http://localhost:3000 -e VUS=100 -e DURATION=10s \
  .k6/email-reservations.js

K6_WEB_DASHBOARD=true \
K6_WEB_DASHBOARD_PORT=-1 \
K6_WEB_DASHBOARD_PERIOD=1s \
K6_WEB_DASHBOARD_EXPORT=.k6/results/request-code-dashboard.html \
k6 run -e BASE_URL=http://localhost:3000 -e VUS=100 -e DURATION=10s \
  .k6/request-verification-code.js
```

Each command produces two complementary HTML reports:

- `*-dashboard.html` is k6's full dashboard export with request-rate, latency,
  and failure charts.
- `*-status.html` is this repository's compact response-status and
  per-outcome latency table.

`K6_WEB_DASHBOARD_PERIOD=1s` ensures that a ten-second smoke run contains
enough samples for dashboard graphs. Port `-1` disables the live dashboard
listener because these commands only need the exported file.

The scripts also print a terminal summary and write JSON. Default custom report
names are `email-reservations-summary.json` and
`email-reservations-status.html` for the first script, and
`request-code-summary.json` and `request-code-status.html` for the second.
All defaults are written under `.k6/results/`. Set `REPORT_FILE` and
`REPORT_HTML` to choose other paths.

Both scripts support `PROFILE=constant-vus` (default) or `PROFILE=ramping-vus`.
`VUS` defaults to 10 and `DURATION` to 10 seconds. For a ramp, set
`START_VUS`, `MAX_VUS`, `RAMP_UP`, and `HOLD`; `GRACEFUL_STOP` controls in-flight
drain time. `EMAIL_DOMAIN` defaults to `load-test.example`; `RUN_ID` supplies
a unique lowercase run prefix. `HTTP_TIMEOUT` defaults to 30 seconds.

The request-code report counts `202`, `409`, `404`, `422`, `5xx`, and
other outcomes separately, including latency for each populated category. A
persistent `409` at high concurrency means the three-attempt conflict budget
was exhausted; `404`, `422`, `5xx`, and timeouts are unexpected for its
reserved-email setup.

Both scripts fail their k6 process when an iteration receives an unexpected
status or response shape. Expected `409` responses in the request-code scenario
do not fail the run.
