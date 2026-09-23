import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const controlled429 = new Counter('controlled_429');
const nonControlledErrors = new Rate('non_controlled_errors');
const bmkgLatency = new Trend('bmkg_only_latency', true);

export const options = {
  vus: 50,
  duration: '60s',
  thresholds: {
    non_controlled_errors: ['rate<0.01'],
    bmkg_only_latency: ['p(95)<300'],
  },
};

export default function () {
  const response = http.get(`${__ENV.PUBLIC_URL || 'http://localhost:8080'}/health`);
  if (response.status === 429) controlled429.add(1);
  nonControlledErrors.add(response.status >= 500 || response.status === 0);
  check(response, { 'health responds': (r) => r.status === 200 || r.status === 429 });

  const started = Date.now();
  const bmkg = http.get(`${__ENV.BMKG_URL || 'http://localhost:8081'}/seismic-events?since=1970-01-01T00:00:00Z`, {
    headers: { 'X-BMKG-Key': __ENV.BMKG_API_KEY || 'example-bmkg-key' },
  });
  bmkgLatency.add(Date.now() - started);
  if (bmkg.status === 429) controlled429.add(1);
  nonControlledErrors.add(bmkg.status >= 500 || bmkg.status === 0);
  check(bmkg, { 'BMKG responds': (r) => r.status === 200 || r.status === 429 });
}
