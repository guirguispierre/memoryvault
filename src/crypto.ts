import type { AccessTokenPayload } from './types.js';
import { PBKDF2_ITERATIONS } from './constants.js';
import { now } from './utils.js';

export function bytesToBase64Url(bytes: Uint8Array): string {
  let bin = '';
  for (const byte of bytes) bin += String.fromCharCode(byte);
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

export function base64UrlToBytes(input: string): Uint8Array {
  const base64 = input.replace(/-/g, '+').replace(/_/g, '/').padEnd(Math.ceil(input.length / 4) * 4, '=');
  const bin = atob(base64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

export async function hmacSha256(secret: string, data: string): Promise<Uint8Array> {
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sig = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(data));
  return new Uint8Array(sig);
}

export async function sha256DigestBase64Url(value: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return bytesToBase64Url(new Uint8Array(digest));
}

export async function derivePasswordHash(password: string, salt: Uint8Array, iterations = PBKDF2_ITERATIONS): Promise<string> {
  const keyMaterial = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(password),
    'PBKDF2',
    false,
    ['deriveBits']
  );
  const bits = await crypto.subtle.deriveBits(
    { name: 'PBKDF2', hash: 'SHA-256', iterations, salt },
    keyMaterial,
    256
  );
  return bytesToBase64Url(new Uint8Array(bits));
}

export async function hashPassword(password: string): Promise<string> {
  const salt = crypto.getRandomValues(new Uint8Array(16));
  const hash = await derivePasswordHash(password, salt, PBKDF2_ITERATIONS);
  return `pbkdf2_sha256$${PBKDF2_ITERATIONS}$${bytesToBase64Url(salt)}$${hash}`;
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [algo, iterRaw, saltRaw, hashRaw] = stored.split('$');
  if (algo !== 'pbkdf2_sha256' || !iterRaw || !saltRaw || !hashRaw) return false;
  const iterations = Number(iterRaw);
  if (!Number.isFinite(iterations) || iterations < 10_000) return false;
  const recomputed = await derivePasswordHash(password, base64UrlToBytes(saltRaw), iterations);
  return recomputed === hashRaw;
}

export function randomToken(size = 32): string {
  const bytes = crypto.getRandomValues(new Uint8Array(size));
  return bytesToBase64Url(bytes);
}

export function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

export function isValidEmail(email: string): boolean {
  const v = normalizeEmail(email);
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) && v.length <= 254;
}

export function isStrongEnoughPassword(password: string): boolean {
  return password.length >= 10;
}

export async function signAccessToken(payload: AccessTokenPayload, secret: string): Promise<string> {
  const header = { alg: 'HS256', typ: 'JWT' };
  const headerPart = bytesToBase64Url(new TextEncoder().encode(JSON.stringify(header)));
  const payloadPart = bytesToBase64Url(new TextEncoder().encode(JSON.stringify(payload)));
  const body = `${headerPart}.${payloadPart}`;
  const sig = await hmacSha256(secret, body);
  return `${body}.${bytesToBase64Url(sig)}`;
}

export async function verifyAccessToken(token: string, secret: string): Promise<AccessTokenPayload | null> {
  const [headerPart, payloadPart, sigPart] = token.split('.');
  if (!headerPart || !payloadPart || !sigPart) return null;
  try {
    const payloadJson = new TextDecoder().decode(base64UrlToBytes(payloadPart));
    const payload = JSON.parse(payloadJson) as Partial<AccessTokenPayload>;
    if (payload.typ !== 'access' || typeof payload.sub !== 'string' || typeof payload.bid !== 'string' || typeof payload.sid !== 'string') {
      return null;
    }
    if (typeof payload.iat !== 'number' || typeof payload.exp !== 'number') return null;
    if (payload.exp < now()) return null;
    const expectedSig = await hmacSha256(secret, `${headerPart}.${payloadPart}`);
    const givenSig = base64UrlToBytes(sigPart);
    if (expectedSig.length !== givenSig.length) return null;
    for (let i = 0; i < expectedSig.length; i++) {
      if (expectedSig[i] !== givenSig[i]) return null;
    }
    return payload as AccessTokenPayload;
  } catch {
    return null;
  }
}
