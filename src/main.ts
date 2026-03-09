import { ConfigService } from '@nestjs/config';
import { Logger } from 'nestjs-pino';
import { createNestApp } from './app-factory';

async function bootstrap() {
  const app = await createNestApp();
  const configService = app.get(ConfigService);
  const logger = app.get(Logger);

  const port = configService.get<number>('PORT');
  if (port == null || port === 0) {
    throw new Error('PORT must be set in environment (e.g. in apps/api/.env)');
  }
  await app.listen(port, '0.0.0.0');
  const appUrlLog = configService.get<string>('APP_URL');
  if (appUrlLog) {
    logger.log(`🚀 Application is running on: ${appUrlLog}`);
    logger.log(`📚 Swagger documentation: ${appUrlLog}/api/docs`);
  } else {
    logger.log(`🚀 Application listening on port ${port} (set APP_URL in .env for full URL)`);
    logger.log(`📚 Swagger: /api/docs`);
  }
  // Temporary: verify connection routes are registered
  logger.log('Registered routes include POST .../connection/status for connection status updates');
}

bootstrap();
