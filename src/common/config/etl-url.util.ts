/**
 * ETL Python Service URL resolution for NestJS API.
 * - When ETL_PYTHON_SERVICE_URL is set, use it (hosted or local).
 * - When not set, use http://localhost:8001. Configure per env via env vars.
 */
import { ConfigService } from '@nestjs/config';

const DEFAULT_LOCAL = 'http://localhost:8001';

/**
 * Resolves the ETL Python service base URL from env.
 * - Reads ETL_PYTHON_SERVICE_URL. If unset, uses localhost:8001.
 * - Normalizes: if value has no scheme, prepends https://.
 */
export function getEtlServiceUrl(configService: ConfigService): string {
  const raw = configService.get<string>('ETL_PYTHON_SERVICE_URL');

  if (!raw || String(raw).trim() === '') {
    return DEFAULT_LOCAL;
  }

  const trimmed = String(raw).trim();
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `https://${trimmed}`;
}
