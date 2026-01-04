import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // Enable validation pipe globally
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  // Swagger/OpenAPI Configuration
  const config = new DocumentBuilder()
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
    )
    .addServer('http://localhost:8000', 'Development server')
    .addServer('https://api.mantrixflow.com', 'Production server')
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('api/docs', app, document, {
    customSiteTitle: 'MantrixFlow API Documentation',
    customfavIcon: '/favicon.ico',
    customCss: '.swagger-ui .topbar { display: none }',
  });

  // Enable CORS for API access
  // When using credentials: 'include', we must specify exact origins, not wildcard
  const allowedOrigins = [
    'http://localhost:3000',
    'http://localhost:3001',
    process.env.FRONTEND_URL,
    process.env.NEXT_PUBLIC_APP_URL,
  ].filter(Boolean) as string[];

  // CORS configuration - must specify exact origins when using credentials
  // Cannot use wildcard '*' when credentials: true
  app.enableCors({
    origin: (origin, callback) => {
      // For preflight OPTIONS requests, origin might be undefined
      // We need to explicitly allow the origin, not return true (which defaults to *)
      if (!origin) {
        // In development, default to localhost:3000 for no-origin requests
        if (process.env.NODE_ENV === 'development') {
          return callback(null, 'http://localhost:3000');
        }
        return callback(new Error('Not allowed by CORS'));
      }

      // Check if origin is in allowed list
      if (allowedOrigins.includes(origin)) {
        callback(null, origin); // Return the specific origin, not true
      } else {
        // In development, allow localhost with any port
        if (process.env.NODE_ENV === 'development' && origin.startsWith('http://localhost:')) {
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

  const port = process.env.PORT ?? 8000;
  await app.listen(port);
  console.log(`🚀 Application is running on: http://localhost:${port}`);
  console.log(`📚 Swagger documentation: http://localhost:${port}/api/docs`);
}

void bootstrap();
