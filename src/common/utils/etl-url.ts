/**
 * Normalizes and validates the ETL Python service base URL from environment.
 * - Strips optional surrounding quotes (some env loaders include them).
 * - Adds https:// when scheme is missing.
 * - Returns only URLs that parse as valid with a non-empty host (so axios never gets "Invalid URL").
 * Use ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL in .env.
 */
export function normalizeEtlBaseUrl(url: string | undefined): string {
  let raw = (url ?? '').trim();
  if (!raw) return '';
  raw = raw.replace(/^["']|["']$/g, '').trim();
  if (!raw) return '';
  const withoutTrailingSlash = raw.replace(/\/+$/, '');
  const withScheme = /^https?:\/\//i.test(withoutTrailingSlash)
    ? withoutTrailingSlash
    : `https://${withoutTrailingSlash}`;
  try {
    const parsed = new URL(`${withScheme}/`);
    if (!parsed.host) return '';
    return withScheme;
  } catch {
    return '';
  }
}
