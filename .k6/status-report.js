export function statusCounts(report, statusMetrics) {
    const counts = {};

    Object.keys(statusMetrics || {}).forEach((status) => {
        const count = metricCount(report, statusMetrics[status]);
        if (count > 0) {
            counts[status] = count;
        }
    });

    return sortStatusMap(counts);
}

export function htmlStatusReport(report, options) {
    const configuration = options || {};
    const test = report.test || {};
    const counts = report.status_counts || {};
    const statuses = Object.keys(counts).sort(compareStatuses);
    const totalRequests = metricCount(report, configuration.requestMetric) || sumCounts(counts);
    const totalResponses = sumCounts(counts);
    const expectedStatuses = (configuration.expectedStatuses || []).map((status) => String(status));
    const durationMetrics = configuration.durationMetrics || {};
    const statusRows = statuses.length > 0
        ? statuses.map((status) => statusRow(
            report,
            status,
            counts[status],
            totalResponses,
            expectedStatuses,
            durationMetrics,
        )).join('')
        : '<tr><td colspan="6">No load-phase responses were recorded.</td></tr>';
    const failureRate = metricRate(report, configuration.failureMetric);
    const checkRate = metricRate(report, configuration.checkMetric);
    const overallDuration = metricValues(report, configuration.overallDurationMetric);
    const title = configuration.title || 'k6 load test status report';
    const expectedLabel = expectedStatuses.length > 0 ? expectedStatuses.join(', ') : 'none';

    return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(title)}</title>
  <style>
    :root { color-scheme: light dark; font-family: system-ui, sans-serif; }
    body { margin: 0; padding: 2rem; background: #f6f8fa; color: #172b4d; }
    main { max-width: 1100px; margin: 0 auto; }
    h1 { margin: 0 0 .35rem; font-size: 1.8rem; }
    h2 { margin-top: 2rem; }
    .muted { color: #5e6c84; }
    .meta, .cards { display: grid; gap: .8rem; }
    .meta { grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); margin: 1.4rem 0; }
    .cards { grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }
    .card, .panel { background: white; border: 1px solid #dfe1e6; border-radius: 8px; padding: 1rem; box-shadow: 0 1px 2px #091e4226; }
    .card strong { display: block; font-size: 1.35rem; margin-top: .25rem; }
    table { width: 100%; border-collapse: collapse; background: white; border: 1px solid #dfe1e6; border-radius: 8px; overflow: hidden; }
    th, td { padding: .75rem; border-bottom: 1px solid #ebecf0; text-align: left; }
    th { background: #ebecf0; font-weight: 650; }
    tr:last-child td { border-bottom: 0; }
    .expected { color: #006644; font-weight: 650; }
    .unexpected { color: #bf2600; font-weight: 650; }
    code, pre { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
    pre { padding: 1rem; overflow: auto; background: #172b4d; color: #fff; border-radius: 6px; }
    @media (prefers-color-scheme: dark) {
      body { background: #101214; color: #dfe1e6; }
      .muted { color: #b6c2cf; }
      .card, .panel, table { background: #1d2125; border-color: #454f59; }
      th { background: #282e33; }
      th, td { border-color: #38414a; }
    }
  </style>
</head>
<body>
<main>
  <h1>${escapeHtml(title)}</h1>
  <div class="muted">Generated ${escapeHtml(test.generated_at || '')}</div>

  <div class="meta">
    <div class="panel"><strong>Endpoint</strong><br>${escapeHtml(test.method || '')} ${escapeHtml(test.endpoint || '')}</div>
    <div class="panel"><strong>Profile</strong><br>${escapeHtml(test.profile || '')} (${escapeHtml(test.duration || '')})</div>
    ${test.target_email ? `<div class="panel"><strong>Target email</strong><br><code>${escapeHtml(test.target_email)}</code></div>` : ''}
  </div>

  <div class="cards">
    <div class="card"><span class="muted">Load requests</span><strong>${formatInteger(totalRequests)}</strong></div>
    <div class="card"><span class="muted">HTTP responses</span><strong>${formatInteger(totalResponses)}</strong></div>
    <div class="card"><span class="muted">HTTP failures</span><strong>${formatPercent(failureRate * 100)}</strong></div>
    <div class="card"><span class="muted">Checks passed</span><strong>${formatPercent(checkRate * 100)}</strong></div>
    <div class="card"><span class="muted">Overall p95</span><strong>${formatMilliseconds(overallDuration['p(95)'])}</strong></div>
  </div>

  <h2>HTTP status breakdown</h2>
  <p class="muted">Load-phase responses only. Expected statuses: ${escapeHtml(expectedLabel)}.</p>
  <table>
    <thead><tr><th>Status</th><th>Requests</th><th>Share</th><th>Result</th><th>p95</th><th>Max</th></tr></thead>
    <tbody>${statusRows}</tbody>
  </table>

  <h2>Exact counts</h2>
  <details>
    <summary>Show <code>status_counts</code> JSON</summary>
    <pre>${escapeHtml(JSON.stringify(counts, null, 2))}</pre>
  </details>
</main>
</body>
</html>
`;
}

function statusRow(report, status, count, totalResponses, expectedStatuses, durationMetrics) {
    const share = totalResponses > 0 ? (count / totalResponses) * 100 : 0;
    const expected = expectedStatuses.indexOf(String(status)) >= 0;
    const values = metricValues(report, durationMetrics[status]);
    const result = expected ? 'expected' : 'unexpected';
    const resultClass = expected ? 'expected' : 'unexpected';

    return `<tr>
      <td><code>${escapeHtml(status)}</code></td>
      <td>${formatInteger(count)}</td>
      <td>${formatPercent(share)}</td>
      <td class="${resultClass}">${result}</td>
      <td>${formatMilliseconds(values['p(95)'])}</td>
      <td>${formatMilliseconds(values.max)}</td>
    </tr>`;
}

function metricValues(report, name) {
    if (!name || !report || !report.metrics || !report.metrics[name]) {
        return {};
    }
    return report.metrics[name].values || {};
}

function metricCount(report, name) {
    return metricValues(report, name).count || 0;
}

function metricRate(report, name) {
    const rate = metricValues(report, name).rate;
    return typeof rate === 'number' ? rate : 0;
}

function sumCounts(counts) {
    return Object.keys(counts).reduce((total, status) => total + Number(counts[status] || 0), 0);
}

function sortStatusMap(counts) {
    return Object.keys(counts)
        .sort(compareStatuses)
        .reduce((result, status) => {
            result[status] = counts[status];
            return result;
        }, {});
}

function compareStatuses(left, right) {
    const leftNumber = Number(left);
    const rightNumber = Number(right);
    if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) {
        return leftNumber - rightNumber;
    }
    if (Number.isFinite(leftNumber)) {
        return -1;
    }
    if (Number.isFinite(rightNumber)) {
        return 1;
    }
    return left.localeCompare(right);
}

function formatInteger(value) {
    return String(Math.round(Number(value) || 0));
}

function formatPercent(value) {
    return `${(Number(value) || 0).toFixed(2)}%`;
}

function formatMilliseconds(value) {
    return typeof value === 'number' ? `${value.toFixed(2)} ms` : '—';
}

function escapeHtml(value) {
    return String(value === undefined || value === null ? '' : value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
