import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const configService = app.get(ConfigService);
  const logger = new Logger('Bootstrap');

  // Set global API prefix
  app.setGlobalPrefix('api');

  // Enable validation pipe globally
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  // All URLs and origins from environment (apps/api/.env) — no hardcoded URLs
  const apiUrl = configService.get<string>('API_URL');
  const devServerUrl = configService.get<string>('DEV_SERVER_URL');
  const frontendUrl = configService.get<string>('FRONTEND_URL');
  const appUrl = configService.get<string>('APP_URL');
  const allowedOriginsEnv = configService.get<string>('ALLOWED_ORIGINS');

  const swaggerConfig = new DocumentBuilder()
    .setTitle('MantrixFlow PostgreSQL Connector API')
    .setDescription(
      'REST API for managing PostgreSQL connections, schema discovery, query execution, and data synchronization.',
    )
    .setVersion('1.0')
    .addTag('postgres', 'PostgreSQL Connector endpoints')
    .addBearerAuth(
      {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT',
        description: 'Enter JWT token',
      },
      'JWT-auth',
    );

  if (devServerUrl) swaggerConfig.addServer(devServerUrl, 'Development server');
  if (apiUrl) swaggerConfig.addServer(apiUrl, 'Production server');

  const document = SwaggerModule.createDocument(app, swaggerConfig.build());
  SwaggerModule.setup('api/docs', app, document, {
    customSiteTitle: 'MantrixFlow API Documentation',
    customfavIcon: '/favicon.ico',
    customCss: '.swagger-ui .topbar { display: none }',
    customCssUrl: 'https://unpkg.com/swagger-ui-dist/swagger-ui.css',
    customJs: [
      'https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js',
      'https://unpkg.com/swagger-ui-dist/swagger-ui-standalone-preset.js',
    ],
  });

  // CORS: only origins from env (ALLOWED_ORIGINS, FRONTEND_URL, APP_URL, API_URL)
  const allowedOrigins: string[] = [];
  if (allowedOriginsEnv) {
    allowedOrigins.push(...allowedOriginsEnv.split(',').map((origin) => origin.trim()));
  }
  if (frontendUrl && !allowedOrigins.includes(frontendUrl)) allowedOrigins.push(frontendUrl);
  if (appUrl && !allowedOrigins.includes(appUrl)) allowedOrigins.push(appUrl);
  if (apiUrl && !allowedOrigins.includes(apiUrl)) allowedOrigins.push(apiUrl);

  app.enableCors({
    origin: (
      origin: string | undefined,
      callback: (err: Error | null, allow?: boolean | string) => void,
    ) => {
      if (!origin) return callback(null, true);
      if (allowedOrigins.includes(origin)) return callback(null, origin);
      return callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  });

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
}

bootstrap();
