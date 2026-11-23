import { test } from 'node:test';
import assert from 'node:assert/strict';
import { allowedPath, forward } from './app.js';
import { SECRET_HEADER } from './index.js';

const request = (body, secret = 's3cret') => ({
  headers: { [SECRET_HEADER]: [secret] },
  body: JSON.stringify(body),
});

test('allows only the app properties route', () => {
  assert.ok(allowedPath('/wiki/api/v2/app/properties'));
  assert.ok(allowedPath('/wiki/api/v2/app/properties?limit=5'));
  assert.ok(allowedPath('/wiki/api/v2/app/properties/my-key'));
  assert.ok(!allowedPath('/wiki/api/v2/app/propertiesX'));
  assert.ok(!allowedPath('/wiki/api/v2/pages'));
  assert.ok(!allowedPath('/wiki/api/v2/app/properties/../../pages'));
  assert.ok(!allowedPath('/wiki/api/v2/app/properties/a/b'));
  assert.ok(!allowedPath('/wiki/api/v2/app/properties//x'));
  assert.ok(!allowedPath(null));
});

test('allows the custom content route, one id under it, and a query', () => {
  assert.ok(allowedPath('/wiki/api/v2/custom-content'));
  assert.ok(allowedPath('/wiki/api/v2/custom-content?type=forge:x&space-id=1'));
  assert.ok(allowedPath('/wiki/api/v2/custom-content/123?purge=true'));
  assert.ok(!allowedPath('/wiki/api/v2/custom-content/123/children'));
  assert.ok(!allowedPath('/wiki/api/v2/custom-contentX'));
  assert.ok(!allowedPath('/wiki/api/v2/custom-content/../pages'));
});

test('POST is forwarded for custom content only', async () => {
  const calls = [];
  const call = async (method, path, body) => {
    calls.push([method, path, body]);
    return { status: 200, body: { id: '1' } };
  };
  const created = await forward(request({ method: 'POST', path: '/wiki/api/v2/custom-content', body: { type: 't' } }),
    's3cret', call);
  assert.equal(created.statusCode, 200);
  assert.deepEqual(calls, [['POST', '/wiki/api/v2/custom-content', { type: 't' }]]);
  const refused = await forward(request({ method: 'POST', path: '/wiki/api/v2/app/properties/k' }), 's3cret', call);
  assert.equal(refused.statusCode, 400);
  assert.equal(calls.length, 1);
});

test('forwards an allowed call and answers status and body', async () => {
  const calls = [];
  const call = async (method, path, body) => {
    calls.push([method, path, body]);
    return { status: 204, body: null };
  };
  const answer = await forward(request({ method: 'put', path: '/wiki/api/v2/app/properties/k', body: { value: 1 } }),
    's3cret', call);
  assert.equal(answer.statusCode, 200);
  assert.deepEqual(JSON.parse(answer.body), { status: 204, body: null });
  assert.deepEqual(calls, [['PUT', '/wiki/api/v2/app/properties/k', { value: 1 }]]);
});

test('refuses a wrong secret, a bad method or a bad path without calling', async () => {
  const call = async () => assert.fail('must not call');
  assert.equal((await forward(request({ method: 'GET', path: '/wiki/api/v2/app/properties' }, 'x'), 's3cret', call))
    .statusCode, 401);
  assert.equal((await forward(request({ method: 'POST', path: '/wiki/api/v2/app/properties' }), 's3cret', call))
    .statusCode, 400);
  assert.equal((await forward(request({ method: 'GET', path: '/wiki/api/v2/spaces' }), 's3cret', call))
    .statusCode, 400);
});
