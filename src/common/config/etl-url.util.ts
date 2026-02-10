/**
 * ETL Python Service URL resolution for NestJS API.
 * Uses only env: ETL_PYTHON_SERVICE_URL. No hardcoded URLs.
 */
import { ConfigService } from '@nestjs/config';

/**
 * Resolves the ETL Python service base URL from env.
 * - Reads ETL_PYTHON_SERVICE_URL (from ConfigService, then process.env).
 * - Normalizes: if value has no scheme, prepends https://.
 * - Throws if unset so the app never uses a wrong URL.
 */
export function getEtlServiceUrl(configService: ConfigService): string {
  const raw =
    configService.get<string>('ETL_PYTHON_SERVICE_URL') ?? process.env.ETL_PYTHON_SERVICE_URL;

  const trimmed = typeof raw === 'string' ? raw.trim() : '';
  if (!trimmed) {
    throw new Error(
      'ETL_PYTHON_SERVICE_URL must be set in the environment (e.g. in .env). ' +
        'Example: ETL_PYTHON_SERVICE_URL=https://cloud.api.etl.server.mantrixflow.com',
    );
  }

  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `https://${trimmed}`;
}
