import http from 'k6/http';
import { check } from 'k6';

export default function () {
  let lockName = `lock_${(Math.random() + 1).toString(36).substring(2)}`;

  const url = `http://localhost:11000/API/v1/locks/${lockName}`;
  let payload = JSON.stringify({
    ttl: 120,
  });

  let params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { name: 'lock' },
  };

  let resp = http.post(url, payload, params);

  check(resp, {
    'locked': (r) => r.status === 200,
  });

  params = {
    headers: {
      'Content-Type': 'application/json',
    },
    tags: { name: 'release' },
  };

  resp = http.request("DELETE", url, {}, params);
  check(resp, {
    'release': (r) => r.status === 200,
  });
}
