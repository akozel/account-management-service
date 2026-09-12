import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Trend } from 'k6/metrics';
import { htmlStatusReport, statusCounts } from './status-report.js';

const baseUrl = (__ENV.BASE_URL || 'http://localhost:3000').replace(/\/+$/, '');
const endpoint = `${baseUrl}/email-reservations`;
const profile = (__ENV.PROFILE || 'constant-vus').trim().toLowerCase();
const duration = __ENV.DURATION || '10s';
const vus = positiveInteger('VUS', 10);
const startVus = nonNegativeInteger('START_VUS', 0);
const maxVus = positiveInteger('MAX_VUS', vus);
const rampUp = __ENV.RAMP_UP || '8s';
const hold = __ENV.HOLD || '2s';
const gracefulStop = __ENV.GRACEFUL_STOP || '5s';
const requestTimeout = __ENV.HTTP_TIMEOUT || '30s';
const emailDomain = (__ENV.EMAIL_DOMAIN || 'load-test.example').trim().toLowerCase();
const generatedRunId = new SharedArray('email reservation generated run ID', () => [
    `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`,
])[0];
const configuredRunId = (__ENV.RUN_ID || '').trim().toLowerCase();
const runId = configuredRunId || generatedRunId;

if (!/^[a-z0-9-]+$/.test(runId)) {
    throw new Error(`RUN_ID must contain only lowercase letters, digits, and hyphens, got: ${runId}`);
}

const acceptedRegistrations = new Counter('registrations_accepted');
const acceptedRegistrationDuration = new Trend('registrations_accepted_duration');
const conflictRegistrations = new Counter('registrations_conflict');
const conflictRegistrationDuration = new Trend('registrations_conflict_duration');
const invalidRegistrations = new Counter('registrations_invalid');
const invalidRegistrationDuration = new Trend('registrations_invalid_duration');
const serverErrorRegistrations = new Counter('registrations_server_error');
const serverErrorRegistrationDuration = new Trend('registrations_server_error_duration');
const otherRegistrations = new Counter('registrations_other');
const otherRegistrationDuration = new Trend('registrations_other_duration');
const registrationCheckFailures = new Counter('registration_check_failures');

export const options = {
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
    thresholds: {
        registration_check_failures: ['count == 0'],
    },
    scenarios: {
        registration: scenarioFor(profile),
    },
};

export default function () {
    const email = `k6-${runId}-${__VU}-${__ITER}@${emailDomain}`;
    const response = http.post(
        endpoint,
        JSON.stringify({ email }),
        {
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            timeout: requestTimeout,
        },
    );

    recordStatus(response.status, response.timings.duration);

    let body;
    if (response.status === 202) {
        try {
            body = response.json();
        } catch (_) {
            body = undefined;
        }
    }

    const passed = check(response, {
        'status is 202 Accepted': (result) => result.status === 202,
        'response contains the requested email': () => body && body.email === email,
        'response contains new account ID': () => body && typeof body.account_id === 'string',
    });

    if (!passed) {
        registrationCheckFailures.add(1);
    }
}

export function handleSummary(data) {
    const reportFile = __ENV.REPORT_FILE || '.k6/results/email-reservations-summary.json';
    const reportHtmlFile = __ENV.REPORT_HTML || '.k6/results/email-reservations-status.html';
    const report = {
        test: {
            generated_at: new Date().toISOString(),
            method: 'POST',
            endpoint,
            profile,
            duration: profile === 'constant-vus' ? duration : `${rampUp}+${hold}`,
            vus: profile === 'constant-vus' ? vus : undefined,
            start_vus: profile === 'ramping-vus' ? startVus : undefined,
            max_vus: profile === 'ramping-vus' ? maxVus : undefined,
            run_id: runId,
        },
        metrics: data.metrics,
        status_counts: statusCounts(data, {
            202: 'registrations_accepted',
            409: 'registrations_conflict',
            422: 'registrations_invalid',
            '5xx': 'registrations_server_error',
            other: 'registrations_other',
        }),
    };

    return {
        stdout: textSummary(report),
        [reportFile]: JSON.stringify(report, null, 2),
        [reportHtmlFile]: htmlStatusReport(report, {
            title: 'k6 email reservation status report',
            requestMetric: 'http_reqs',
            durationMetrics: {
                202: 'registrations_accepted_duration',
                409: 'registrations_conflict_duration',
                422: 'registrations_invalid_duration',
                '5xx': 'registrations_server_error_duration',
                other: 'registrations_other_duration',
            },
            overallDurationMetric: 'http_req_duration',
            failureMetric: 'http_req_failed',
            checkMetric: 'checks',
            expectedStatuses: [202],
        }),
    };
}

function scenarioFor(selectedProfile) {
    if (selectedProfile === 'constant-vus') {
        return {
            executor: 'constant-vus',
            vus,
            duration,
            gracefulStop,
        };
    }

    if (selectedProfile === 'ramping-vus') {
        if (startVus > maxVus) {
            throw new Error(`START_VUS cannot exceed MAX_VUS (${startVus} > ${maxVus})`);
        }

        return {
            executor: 'ramping-vus',
            startVUs: startVus,
            stages: [
                { duration: rampUp, target: maxVus },
                { duration: hold, target: maxVus },
            ],
            gracefulRampDown: gracefulStop,
            gracefulStop,
        };
    }

    throw new Error(`PROFILE must be constant-vus or ramping-vus, got: ${selectedProfile}`);
}

function positiveInteger(name, fallback) {
    const raw = __ENV[name];
    if (raw === undefined || raw === '') {
        return fallback;
    }

    const value = Number(raw);
    if (!Number.isInteger(value) || value < 1) {
        throw new Error(`${name} must be a positive integer, got: ${raw}`);
    }
    return value;
}

function nonNegativeInteger(name, fallback) {
    const raw = __ENV[name];
    if (raw === undefined || raw === '') {
        return fallback;
    }

    const value = Number(raw);
    if (!Number.isInteger(value) || value < 0) {
        throw new Error(`${name} must be a non-negative integer, got: ${raw}`);
    }
    return value;
}

function recordStatus(status, durationMs) {
    if (status === 202) {
        acceptedRegistrations.add(1);
        acceptedRegistrationDuration.add(durationMs);
    } else if (status === 409) {
        conflictRegistrations.add(1);
        conflictRegistrationDuration.add(durationMs);
    } else if (status === 422) {
        invalidRegistrations.add(1);
        invalidRegistrationDuration.add(durationMs);
    } else if (status >= 500 && status <= 599) {
        serverErrorRegistrations.add(1);
        serverErrorRegistrationDuration.add(durationMs);
    } else {
        otherRegistrations.add(1);
        otherRegistrationDuration.add(durationMs);
    }
}

function metricValues(report, name) {
    const metric = report.metrics[name];
    return metric && metric.values ? metric.values : {};
}

function count(report, name) {
    return metricValues(report, name).count || 0;
}

function fixed(valueToFormat) {
    return valueToFormat.toFixed(2);
}

function textSummary(report) {
    const requests = metricValues(report, 'http_reqs');
    const durationValues = metricValues(report, 'http_req_duration');
    const checks = metricValues(report, 'checks');
    const httpFailures = metricValues(report, 'http_req_failed');

    return [
        'k6 email reservation stress test',
        `Endpoint: ${report.test.method} ${report.test.endpoint}`,
        `Profile: ${report.test.profile} (${report.test.duration})`,
        `Requests: ${count(report, 'http_reqs')} (${fixed(requests.rate || 0)} req/s)`,
        `Accepted (202): ${count(report, 'registrations_accepted')}`,
        `Conflicts (409): ${count(report, 'registrations_conflict')}`,
        `Invalid (422): ${count(report, 'registrations_invalid')}`,
        `Server errors (5xx): ${count(report, 'registrations_server_error')}`,
        `Other responses: ${count(report, 'registrations_other')}`,
        `HTTP status counts: ${formatStatusCounts(report.status_counts)}`,
        `HTTP failures: ${fixed((httpFailures.rate || 0) * 100)}%`,
        `Assertions passed: ${fixed((checks.rate || 0) * 100)}%`,
        `Iterations with check failures: ${count(report, 'registration_check_failures')}`,
        `Latency: p50=${fixed(durationValues.med || 0)} ms, `
            + `p95=${fixed(durationValues['p(95)'] || 0)} ms, `
            + `p99=${fixed(durationValues['p(99)'] || 0)} ms, `
            + `max=${fixed(durationValues.max || 0)} ms`,
    ].join('\n') + '\n';
}

function formatStatusCounts(counts) {
    const statuses = Object.keys(counts || {});
    return statuses.length > 0
        ? statuses.map((status) => `${status}=${counts[status]}`).join(', ')
        : 'none';
}
