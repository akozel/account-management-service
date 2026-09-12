import http from 'k6/http';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Counter, Trend } from 'k6/metrics';
import { htmlStatusReport, statusCounts } from './status-report.js';

const baseUrl = (__ENV.BASE_URL || 'http://localhost:3000').replace(/\/+$/, '');
const registrationEndpoint = `${baseUrl}/email-reservations`;
const requestCodeEndpoint = `${baseUrl}/email-reservations/{email}/verification-codes`;
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
const generatedRunId = new SharedArray('request code generated run ID', () => [
    `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`,
])[0];
const configuredRunId = (__ENV.RUN_ID || '').trim().toLowerCase();
const runId = configuredRunId || generatedRunId;
const targetEmail = `k6-${runId}-shared@${emailDomain}`;

if (!/^[a-z0-9-]+$/.test(runId)) {
    throw new Error(`RUN_ID must contain only lowercase letters, digits, and hyphens, got: ${runId}`);
}

const setupResponseCallback = http.expectedStatuses(202);
const requestCodeResponseCallback = http.expectedStatuses(202, 409);

const setupRegistrationAccepted = new Counter('setup_registration_accepted');
const setupRegistrationFailures = new Counter('setup_registration_failures');
const setupRegistrationDuration = new Trend('setup_registration_duration');
const requestCodeRequests = new Counter('request_code_requests');
const requestCodeDuration = new Trend('request_code_duration');
const requestCodeAccepted = new Counter('request_code_accepted');
const requestCodeAcceptedDuration = new Trend('request_code_accepted_duration');
const requestCodeConflicts = new Counter('request_code_conflicts');
const requestCodeConflictDuration = new Trend('request_code_conflict_duration');
const requestCodeNotFound = new Counter('request_code_not_found');
const requestCodeNotFoundDuration = new Trend('request_code_not_found_duration');
const requestCodeInvalid = new Counter('request_code_invalid');
const requestCodeInvalidDuration = new Trend('request_code_invalid_duration');
const requestCodeServerErrors = new Counter('request_code_server_errors');
const requestCodeServerErrorDuration = new Trend('request_code_server_error_duration');
const requestCodeOther = new Counter('request_code_other');
const requestCodeOtherDuration = new Trend('request_code_other_duration');
const requestCodeCheckFailures = new Counter('request_code_check_failures');

export const options = {
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
    thresholds: {
        request_code_check_failures: ['count == 0'],
    },
    scenarios: {
        request_code: scenarioFor(profile),
    },
};

export function setup() {
    const response = http.post(
        registrationEndpoint,
        JSON.stringify({ email: targetEmail }),
        {
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            timeout: requestTimeout,
            tags: {
                name: 'register-email-precondition',
                phase: 'setup',
            },
            responseCallback: setupResponseCallback,
        },
    );

    setupRegistrationDuration.add(response.timings.duration);

    if (response.status !== 202) {
        setupRegistrationFailures.add(1);
        throw new Error(`email precondition failed with HTTP ${response.status}`);
    }

    let body;
    try {
        body = response.json();
    } catch (_) {
        body = undefined;
    }

    if (!body || body.email !== targetEmail || typeof body.account_id !== 'string') {
        setupRegistrationFailures.add(1);
        throw new Error('email precondition returned an unexpected response');
    }

    setupRegistrationAccepted.add(1);
    return { email: targetEmail };
}

export default function (data) {
    const email = data.email;
    const endpoint = `${baseUrl}/email-reservations/${encodeURIComponent(email)}/verification-codes`;
    const response = http.post(
        endpoint,
        null,
        {
            headers: {
                Accept: 'application/json',
            },
            timeout: requestTimeout,
            tags: {
                name: 'request-new-code',
                phase: 'load',
            },
            responseCallback: requestCodeResponseCallback,
        },
    );

    requestCodeRequests.add(1);
    requestCodeDuration.add(response.timings.duration);
    recordStatus(response.status, response.timings.duration);

    const passed = check(response, {
        'status is 202 Accepted or expected 409 Conflict': (result) =>
            result.status === 202 || result.status === 409,
    });

    if (!passed) {
        requestCodeCheckFailures.add(1);
    }
}

export function handleSummary(data) {
    const reportFile = __ENV.REPORT_FILE || '.k6/results/request-code-summary.json';
    const reportHtmlFile = __ENV.REPORT_HTML || '.k6/results/request-code-status.html';
    const report = {
        test: {
            generated_at: new Date().toISOString(),
            precondition: `POST ${registrationEndpoint}`,
            method: 'POST',
            endpoint: requestCodeEndpoint,
            target_email: targetEmail,
            profile,
            duration: profile === 'constant-vus' ? duration : `${rampUp}+${hold}`,
            vus: profile === 'constant-vus' ? vus : undefined,
            start_vus: profile === 'ramping-vus' ? startVus : undefined,
            max_vus: profile === 'ramping-vus' ? maxVus : undefined,
            run_id: runId,
        },
        metrics: data.metrics,
        status_counts: statusCounts(data, {
            202: 'request_code_accepted',
            409: 'request_code_conflicts',
            404: 'request_code_not_found',
            422: 'request_code_invalid',
            '5xx': 'request_code_server_errors',
            other: 'request_code_other',
        }),
    };

    return {
        stdout: textSummary(report),
        [reportFile]: JSON.stringify(report, null, 2),
        [reportHtmlFile]: htmlStatusReport(report, {
            title: 'k6 request verification code status report',
            requestMetric: 'request_code_requests',
            durationMetrics: {
                202: 'request_code_accepted_duration',
                409: 'request_code_conflict_duration',
                404: 'request_code_not_found_duration',
                422: 'request_code_invalid_duration',
                '5xx': 'request_code_server_error_duration',
                other: 'request_code_other_duration',
            },
            overallDurationMetric: 'request_code_duration',
            failureMetric: 'http_req_failed',
            checkMetric: 'checks',
            expectedStatuses: [202, 409],
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
        requestCodeAccepted.add(1);
        requestCodeAcceptedDuration.add(durationMs);
    } else if (status === 409) {
        requestCodeConflicts.add(1);
        requestCodeConflictDuration.add(durationMs);
    } else if (status === 404) {
        requestCodeNotFound.add(1);
        requestCodeNotFoundDuration.add(durationMs);
    } else if (status === 422) {
        requestCodeInvalid.add(1);
        requestCodeInvalidDuration.add(durationMs);
    } else if (status >= 500 && status <= 599) {
        requestCodeServerErrors.add(1);
        requestCodeServerErrorDuration.add(durationMs);
    } else {
        requestCodeOther.add(1);
        requestCodeOtherDuration.add(durationMs);
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
    const requests = metricValues(report, 'request_code_requests');
    const durationValues = metricValues(report, 'request_code_duration');
    const setupDurationValues = metricValues(report, 'setup_registration_duration');
    const checks = metricValues(report, 'checks');
    const httpFailures = metricValues(report, 'http_req_failed');
    const exactStatusCounts = report.status_counts;
    const statusLine = Object.keys(exactStatusCounts).length > 0
        ? Object.keys(exactStatusCounts)
            .map((status) => `${status}=${exactStatusCounts[status]}`)
            .join(', ')
        : 'none';

    return [
        'k6 request verification code stress test',
        `Precondition: ${report.test.precondition}`,
        `Endpoint: ${report.test.method} ${report.test.endpoint}`,
        `Target email: ${report.test.target_email}`,
        `Profile: ${report.test.profile} (${report.test.duration})`,
        `Setup registration: ${count(report, 'setup_registration_accepted')} accepted, `
            + `${count(report, 'setup_registration_failures')} failures, `
            + `${fixed(setupDurationValues.avg || 0)} ms`,
        `Load requests: ${count(report, 'request_code_requests')} (${fixed(requests.rate || 0)} req/s)`,
        `Accepted (202): ${count(report, 'request_code_accepted')}`,
        `Conflicts (409): ${count(report, 'request_code_conflicts')}`,
        `Not found (404): ${count(report, 'request_code_not_found')}`,
        `Invalid (422): ${count(report, 'request_code_invalid')}`,
        `Server errors (5xx): ${count(report, 'request_code_server_errors')}`,
        `Other responses: ${count(report, 'request_code_other')}`,
        `HTTP status counts: ${statusLine}`,
        `Unexpected HTTP failures: ${fixed((httpFailures.rate || 0) * 100)}%`,
        `Assertions passed: ${fixed((checks.rate || 0) * 100)}%`,
        `Iterations with check failures: ${count(report, 'request_code_check_failures')}`,
        `Load latency: p50=${fixed(durationValues.med || 0)} ms, `
            + `p95=${fixed(durationValues['p(95)'] || 0)} ms, `
            + `p99=${fixed(durationValues['p(99)'] || 0)} ms, `
            + `max=${fixed(durationValues.max || 0)} ms`,
    ].join('\n') + '\n';
}
