import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const configService = app.get(ConfigService);

  // Trust proxy for ngrok and reverse proxies (important for webhooks)
  // Access Express instance to set trust proxy
  // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
  const expressApp = app.getHttpAdapter().getInstance() as any;
  // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call
  expressApp.set('trust proxy', true);

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

  // Get environment variables
  const nodeEnv = configService.get<string>('NODE_ENV', 'development');
  const apiUrl = configService.get<string>('API_URL');
  const devServerUrl = configService.get<string>('DEV_SERVER_URL', 'http://localhost:8000');
  const frontendUrl = configService.get<string>('FRONTEND_URL');
  const nextPublicAppUrl = configService.get<string>('APP_URL');
  const allowedOriginsEnv = configService.get<string>('ALLOWED_ORIGINS');
  const defaultDevOrigin = configService.get<string>('DEFAULT_DEV_ORIGIN', 'http://localhost:3000');

  // Swagger/OpenAPI Configuration
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

  // Add servers based on environment
  if (nodeEnv === 'development') {
    swaggerConfig.addServer(devServerUrl, 'Development server');
  }
  if (apiUrl) {
    swaggerConfig.addServer(apiUrl, 'Production server');
  }

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

  // Enable CORS for API access
  const allowedOrigins: string[] = [];

  // Add origins from ALLOWED_ORIGINS env var (comma-separated)
  if (allowedOriginsEnv) {
    allowedOrigins.push(...allowedOriginsEnv.split(',').map((origin) => origin.trim()));
  }

  // Add individual origin env vars
  if (frontendUrl) {
    allowedOrigins.push(frontendUrl);
  }
  if (nextPublicAppUrl) {
    allowedOrigins.push(nextPublicAppUrl);
  }

  // Add API URL itself (for same-origin requests)
  if (apiUrl) {
    allowedOrigins.push(apiUrl);
  }

  // In development, add default localhost origins
  if (nodeEnv === 'development') {
    if (!allowedOrigins.includes(defaultDevOrigin)) {
      allowedOrigins.push(defaultDevOrigin);
    }
    // Add localhost:3001 for development
    const devOrigin2 = 'http://localhost:3001';
    if (!allowedOrigins.includes(devOrigin2)) {
      allowedOrigins.push(devOrigin2);
    }
  }

  // CORS configuration
  app.enableCors({
    origin: (
      origin: string | undefined,
      callback: (err: Error | null, allow?: boolean | string) => void,
    ) => {
      // Allow requests with no origin (direct browser access, Postman, curl, etc.)
      if (!origin) {
        return callback(null, true);
      }

      // Check if origin is in allowed list
      if (allowedOrigins.includes(origin)) {
        return callback(null, origin);
      }

      // Allow Vercel domains (production and preview)
      if (
        origin.includes('.vercel.app') ||
        origin.includes('.vercel.sh') ||
        origin.startsWith('https://vercel.live')
      ) {
        return callback(null, origin);
      }

      // Allow production domain (mantrixflow.com)
      if (origin === 'https://mantrixflow.com' || origin === 'http://mantrixflow.com') {
        return callback(null, origin);
      }

      // In development, allow localhost with any port
      if (nodeEnv === 'development' && origin.startsWith('http://localhost:')) {
        return callback(null, origin);
      }

      // Allow same-origin requests (when origin matches API URL)
      if (apiUrl && origin === apiUrl) {
        return callback(null, origin);
      }

      // Reject all other origins
      return callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    preflightContinue: false,
    optionsSuccessStatus: 204,
  });

  const port = configService.get<number>('PORT', 8000);
  await app.listen(port);
  console.log(`🚀 Application is running on: http://localhost:${port}`);
  console.log(`📚 Swagger documentation: http://localhost:${port}/api/docs`);
  console.log(`🔔 Webhook endpoint: http://localhost:${port}/api/billing/webhook`);
}

void bootstrap();
