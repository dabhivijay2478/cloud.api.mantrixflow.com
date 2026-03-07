/**
 * Sanitize ETL error messages before storing in DB or sending to frontend.
 * Tap/target stderr can be very long (full tracebacks, ANSI codes).
 * Strips credentials, hostnames, and file paths to avoid leaking sensitive data.
 */

const ANSI_ESCAPE = /\x1b\[[0-9;]*m/g;
const MAX_LENGTH = 1000;

/** Patterns that may expose sensitive data - replace with redacted placeholder */
const SENSITIVE_PATTERNS: Array<{ pattern: RegExp; replacement: string }> = [
  { pattern: /password['"]?\s*[:=]\s*['"]?[^\s'"]+['"]?/gi, replacement: 'password=***' },
  { pattern: /postgresql[^:]*:\/\/[^:]+:[^@]+@/gi, replacement: 'postgresql://***:***@' },
  { pattern: /\/[^\s]+\.py/g, replacement: '/***.py' },
  { pattern: /\/Users\/[^\s]+\//g, replacement: '/***/' },
  { pattern: /\/home\/[^\s]+\//g, replacement: '/***/' },
  { pattern: /[a-zA-Z0-9-]+\.supabase\.co/g, replacement: '***.supabase.co' },
];

/**
 * Sanitize and truncate an ETL error string.
 * - Strips ANSI escape codes
 * - Redacts passwords, connection strings, file paths, hostnames
 * - Collapses repeated newlines to max 2
 * - Truncates to MAX_LENGTH chars
 */
export function sanitizeEtlError(raw: string | null | undefined): string | undefined {
  if (raw == null || typeof raw !== 'string') return undefined;
  const trimmed = raw.trim();
  if (!trimmed) return undefined;

  let sanitized = trimmed.replace(ANSI_ESCAPE, '');
  for (const { pattern, replacement } of SENSITIVE_PATTERNS) {
    sanitized = sanitized.replace(pattern, replacement);
  }
  sanitized = sanitized.replace(/\n{3,}/g, '\n\n');
  if (sanitized.length > MAX_LENGTH) {
    sanitized = sanitized.substring(0, MAX_LENGTH) + '\n...(truncated)';
  }
  return sanitized;
}
