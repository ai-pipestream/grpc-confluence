import api, { assumeTrustedRoute } from '@forge/api';
import { authorized } from './index.js';

/** The app properties route the app-api trigger forwards. */
export const APP_PROPERTIES = '/wiki/api/v2/app/properties';
/** The custom content route the app-api trigger forwards. */
export const CUSTOM_CONTENT = '/wiki/api/v2/custom-content';
/** Methods the app-api trigger forwards, by route. */
export const ROUTES = new Map([
  [APP_PROPERTIES, new Set(['GET', 'PUT', 'DELETE'])],
  [CUSTOM_CONTENT, new Set(['GET', 'POST', 'PUT', 'DELETE'])],
]);

/** The forwarded route {@code path} falls under: the route, one id or key under it, or either with a query. */
export function routeOf(path) {
  if (typeof path !== 'string' || path.includes('#')) {
    return null;
  }
  for (const base of ROUTES.keys()) {
    if (!path.startsWith(base)) {
      continue;
    }
    const rest = path.slice(base.length);
    if (rest !== '' && !rest.startsWith('/') && !rest.startsWith('?')) {
      continue;
    }
    const route = rest.split('?')[0];
    if (!route.includes('//') && !route.includes('..') && route.split('/').length <= 2) {
      return base;
    }
  }
  return null;
}

/** True when {@code path} falls under a forwarded route. */
export function allowedPath(path) {
  return routeOf(path) !== null;
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
  const route = routeOf(body.path);
  if (route === null) {
    return answer(400, { error: `path must be ${[...ROUTES.keys()].join(' or ')}, one id under it, or either with a query` });
  }
  if (!ROUTES.get(route).has(method)) {
    return answer(400, { error: `method must be one of ${[...ROUTES.get(route)].join(', ')} for ${route}` });
  }
  return answer(200, await call(method, body.path, body.body ?? null));
}

/** Web trigger: forwards app property and custom content calls to Confluence as the app. */
export async function appApi(request) {
  try {
    return await forward(request, process.env.QUEUE_SECRET, asApp);
  } catch (error) {
    console.error(error);
    return answer(500, { error: `${error?.name ?? 'Error'}: ${error?.message ?? error}` });
  }
}
