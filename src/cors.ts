import type { CorsJsonResponseOptions } from './types.js';
import { buildCorsJsonHeaders, normalizeCorsJsonResponseOptions, oauthChallengeHeader } from './utils.js';

export const ALLOWED_ORIGINS = [
  'https://claude.ai',
  'https://poke.com',
];

export const CORS_HEADERS: Record<string, string> = {
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, Accept',
};

export const HTML_SECURITY_HEADERS: Record<string, string> = {
  'X-Frame-Options': 'DENY',
  'X-Content-Type-Options': 'nosniff',
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains; preload',
  'Referrer-Policy': 'no-referrer',
  'Permissions-Policy': 'geolocation=(), microphone=(), camera=()',
  'Content-Security-Policy': "default-src 'self'; script-src 'self' cdn.jsdelivr.net fonts.googleapis.com fonts.gstatic.com; style-src 'self' 'unsafe-inline' fonts.googleapis.com; font-src fonts.gstatic.com; connect-src 'self'; frame-ancestors 'none';",
};

export function corsJsonResponse(
  body: unknown,
  status = 200,
  options: CorsJsonResponseOptions | Record<string, string> = {}
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: buildCorsJsonHeaders(CORS_HEADERS, normalizeCorsJsonResponseOptions(options)),
  });
}

export function getCorsOrigin(request: Request): string {
  const origin = request.headers.get('Origin')?.trim();
  if (origin && ALLOWED_ORIGINS.includes(origin)) return origin;

  const requestOrigin = new URL(request.url).origin;
  return ALLOWED_ORIGINS.includes(requestOrigin) ? requestOrigin : ALLOWED_ORIGINS[0];
}

export function mergeVaryHeader(existingValue: string | null, value: string): string {
  const varyValues = new Set(
    (existingValue ?? '')
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  );
  varyValues.add(value);
  return Array.from(varyValues).join(', ');
}

export function applyCors(request: Request, response: Response): Response {
  const headers = new Headers(response.headers);
  headers.set('Access-Control-Allow-Origin', getCorsOrigin(request));
  for (const [name, value] of Object.entries(CORS_HEADERS)) {
    headers.set(name, value);
  }
  headers.set('Vary', mergeVaryHeader(headers.get('Vary'), 'Origin'));

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export function isHtmlResponse(response: Response): boolean {
  const contentType = (response.headers.get('Content-Type') ?? '').toLowerCase();
  return contentType.includes('text/html');
}

export function wrapWithSecurityHeaders(response: Response): Response {
  const clonedResponse = response.clone();
  const headers = new Headers(clonedResponse.headers);
  for (const [name, value] of Object.entries(HTML_SECURITY_HEADERS)) {
    headers.set(name, value);
  }
  return new Response(clonedResponse.body, {
    status: clonedResponse.status,
    statusText: clonedResponse.statusText,
    headers,
  });
}

export function unauthorized(url?: URL): Response {
  const headers: Record<string, string> = { ...CORS_HEADERS, 'Content-Type': 'application/json' };
  if (url) headers['WWW-Authenticate'] = oauthChallengeHeader(url);
  return new Response(JSON.stringify({ error: 'Unauthorized' }), {
    status: 401,
    headers,
  });
}
