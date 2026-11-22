import api, { assumeTrustedRoute } from '@forge/api';
import { authorized } from './index.js';

/** The only Confluence route the app-api trigger forwards. */
export const APP_PROPERTIES = '/wiki/api/v2/app/properties';
/** Methods the app-api trigger forwards. */
export const METHODS = new Set(['GET', 'PUT', 'DELETE']);

/** True for {@link APP_PROPERTIES} itself, a key under it, or either with a query. */
export function allowedPath(path) {
  if (typeof path !== 'string' || !path.startsWith(APP_PROPERTIES)) {
    return false;
  }
  const rest = path.slice(APP_PROPERTIES.length);
  if (rest !== '' && !rest.startsWith('/') && !rest.startsWith('?')) {
    return false;
  }
  const route = rest.split('?')[0];
  return !route.includes('//') && !route.includes('..') && !path.includes('#')
    && route.split('/').length <= 2;
}

function answer(statusCode, body) {
  return {
    statusCode,
    headers: { 'Content-Type': ['application/json'] },
    body: JSON.stringify(body),
  };
}

/** Calls Confluence as the app and answers its status and JSON body. */
async function asApp(method, path, body) {
  const init = { method, headers: { Accept: 'application/json' } };
  if (body !== null && body !== undefined) {
    init.headers['Content-Type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const response = await api.asApp().requestConfluence(assumeTrustedRoute(path), init);
  const text = await response.text();
  let parsed = null;
  if (text) {
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = text;
    }
  }
  return { status: response.status, body: parsed };
}

/** Answers one forward request {"method","path","body"} with {"status","body"}. */
export async function forward(request, secret, call) {
  if (!authorized(request?.headers, secret)) {
    return answer(401, { error: 'unauthorized' });
  }
  let body;
  try {
    body = JSON.parse(request.body || '{}');
  } catch {
    return answer(400, { error: 'body is not JSON' });
  }
  const method = typeof body.method === 'string' ? body.method.toUpperCase() : '';
  if (!METHODS.has(method)) {
    return answer(400, { error: 'method must be GET, PUT or DELETE' });
  }
  if (!allowedPath(body.path)) {
    return answer(400, { error: `path must be ${APP_PROPERTIES}, a key under it, or either with a query` });
  }
  return answer(200, await call(method, body.path, body.body ?? null));
}

/** Web trigger: forwards app property calls to Confluence as the app. */
export async function appApi(request) {
  try {
    return await forward(request, process.env.QUEUE_SECRET, asApp);
  } catch (error) {
    console.error(error);
    return answer(500, { error: `${error?.name ?? 'Error'}: ${error?.message ?? error}` });
  }
}
