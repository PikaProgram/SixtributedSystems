import http from 'k6/http';
import { Counter, Rate } from 'k6/metrics';

const controlled429 = new Counter('controlled_429');
const nonControlledErrors = new Rate('non_controlled_errors');

const VUS = Number(__ENV.VUS || 10000);
const DURATION = __ENV.DURATION || '120s';

export const options = {
  stages: [
    { duration: '30s', target: VUS },
    { duration: DURATION, target: VUS },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    non_controlled_errors: ['rate<0.01'],
  },
};

const BASE = __ENV.PUBLIC_URL || 'http://localhost:8080';
const authHeaders = (t) => (t ? { Authorization: `Bearer ${t}` } : {});

// shortcut: collision-safe only within a test run, upgrade to uuid if keys persist server-side
const idemKey = () => `k6-${Date.now()}-${Math.random().toString(36).slice(2)}`;

export default function () {
  const roll = Math.random();
  let res;
  if (roll < 0.7) {
    // media consumer: 429s here are controlled (rate-limited by design)
    res = http.get(`${BASE}/api/hazards`, {
      headers: authHeaders(__ENV.TOKEN_MEDIA),
      tags: { auth: 'media' },
    });
    if (res.status === 429) controlled429.add(1);
  } else if (roll < 0.9) {
    res = http.get(`${BASE}/api/hazards`, {
      headers: authHeaders(__ENV.TOKEN_FIELD),
      tags: { auth: 'field' },
    });
  } else {
    res = http.post(
      `${BASE}/api/field-reports`,
      JSON.stringify({
        hazardId: `haz-${Math.floor(Math.random() * 1000)}`,
        note: `load-test-${idemKey()}`,
      }),
      {
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': idemKey() },
        tags: { auth: 'field-report' },
      },
    );
  }
  // status 0 = connection error
  nonControlledErrors.add(res.status >= 500 || res.status === 0);
}
