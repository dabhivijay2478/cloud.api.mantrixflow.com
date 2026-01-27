/**
 * ETL Python Service URL resolution for NestJS API.
 * Ensures hosted deployments use ETL_PYTHON_SERVICE_URL (e.g. https://cloud.api.etl.server.mantrixflow.com)
 * instead of falling back to localhost.
 */
import { ConfigService } from '@nestjs/config';

const DEFAULT_LOCAL = 'http://localhost:8001';

/**
 * Resolves the ETL Python service base URL from env.
 * - Reads ETL_PYTHON_SERVICE_URL or PYTHON_SERVICE_URL.
 * - In production, throws if not set (avoids pointing at localhost on the server).
 * - Normalizes: if value has no scheme, prepends https://.
 */
export function getEtlServiceUrl(configService: ConfigService): string {
  const raw =
    configService.get<string>('ETL_PYTHON_SERVICE_URL') ||
    configService.get<string>('PYTHON_SERVICE_URL');
  const nodeEnv = configService.get<string>('NODE_ENV') ?? 'development';
  const isProduction = nodeEnv === 'production';

  if (!raw || String(raw).trim() === '') {
    if (isProduction) {
      throw new Error(
        'ETL_PYTHON_SERVICE_URL must be set in production. ' +
          'Set it to your hosted ETL base URL (e.g. https://cloud.api.etl.server.mantrixflow.com).',
      );
    }
    return DEFAULT_LOCAL;
  }

  const trimmed = String(raw).trim();
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `https://${trimmed}`;
}
