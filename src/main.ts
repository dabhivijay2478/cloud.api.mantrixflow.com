import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const configService = app.get(ConfigService);

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
  const nextPublicAppUrl = configService.get<string>('NEXT_PUBLIC_APP_URL');
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
  });

  // Enable CORS for API access
  // When using credentials: 'include', we must specify exact origins, not wildcard
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

  // CORS configuration - must specify exact origins when using credentials
  // Cannot use wildcard '*' when credentials: true
  app.enableCors({
    origin: (
      origin: string | undefined,
      callback: (err: Error | null, allow?: boolean | string) => void,
    ) => {
      // For preflight OPTIONS requests, origin might be undefined
      // We need to explicitly allow the origin, not return true (which defaults to *)
      if (!origin) {
        // In development, default to configured dev origin for no-origin requests
        if (nodeEnv === 'development') {
          return callback(null, defaultDevOrigin);
        }
        return callback(new Error('Not allowed by CORS'));
      }

      // Check if origin is in allowed list
      if (allowedOrigins.includes(origin)) {
        callback(null, origin); // Return the specific origin, not true
      } else {
        // In development, allow localhost with any port
        if (nodeEnv === 'development' && origin.startsWith('http://localhost:')) {
          callback(null, origin); // Return the specific origin
        } else {
          callback(new Error('Not allowed by CORS'));
        }
      }
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
}

void bootstrap();
