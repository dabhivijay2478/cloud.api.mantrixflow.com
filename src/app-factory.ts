/**
 * Creates and configures the NestJS application (without listening).
 * Used by main.ts (local/server) and vercel.ts (Vercel serverless).
 */
import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { Logger } from 'nestjs-pino';
import { AppModule } from './app.module';
import { HttpExceptionFilter } from './common/filters/http-exception.filter';

export async function createNestApp() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });

  const pinoLogger = app.get(Logger);
  app.useLogger(pinoLogger);

  const configService = app.get(ConfigService);

  app.setGlobalPrefix('api');
  app.useGlobalFilters(new HttpExceptionFilter());
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

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
      { type: 'http', scheme: 'bearer', bearerFormat: 'JWT', description: 'Enter JWT token' },
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

  const allowedOrigins: string[] = [];
  if (allowedOriginsEnv) {
    allowedOrigins.push(...allowedOriginsEnv.split(',').map((o) => o.trim()));
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
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'x-internal-token'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  });

  return app;
}
