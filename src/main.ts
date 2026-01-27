import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { INestApplication } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import type { Application, Request, Response } from 'express';
import { AppModule } from './app.module';

let cachedApp: INestApplication | null = null;

async function createApp(): Promise<INestApplication> {
  if (cachedApp) {
    return cachedApp;
  }

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

  // Get environment variables (no NODE_ENV – local/hosted use different env vars)
  const apiUrl = configService.get<string>('API_URL');
  const devServerUrl = configService.get<string>('DEV_SERVER_URL');
  const frontendUrl = configService.get<string>('FRONTEND_URL');
  const nextPublicAppUrl = configService.get<string>('APP_URL');
  const allowedOriginsEnv = configService.get<string>('ALLOWED_ORIGINS');
  const defaultDevOrigin = configService.get<string>('DEFAULT_DEV_ORIGIN', 'http://localhost:3000');
  const allowLocalhost = configService.get<string>('ALLOW_LOCALHOST') === 'true';

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

  // Add servers from env (dev and/or prod)
  if (devServerUrl) {
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

  // Add default localhost origins when ALLOW_LOCALHOST is set (e.g. local)
  if (allowLocalhost) {
    if (!allowedOrigins.includes(defaultDevOrigin)) {
      allowedOrigins.push(defaultDevOrigin);
    }
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

      // Allow production domain (mantrixflow.com and cloud subdomains)
      if (
        origin === 'https://mantrixflow.com' ||
        origin === 'http://mantrixflow.com' ||
        origin.includes('cloud.mantrixflow.com') ||
        origin.includes('cloud.api.mantrixflow.com') ||
        origin.includes('cloud.api.etl.server.mantrixflow.com')
      ) {
        return callback(null, origin);
      }

      // Allow localhost when ALLOW_LOCALHOST is set (e.g. local)
      if (allowLocalhost && origin.startsWith('http://localhost:')) {
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

  await app.init();

  // Fallback for unmatched API routes: small "running" response (no error)
  const expressInstance = app.getHttpAdapter().getInstance() as Application;
  expressInstance.use((_req: Request, res: Response) => {
    if (!res.headersSent) {
      res.status(200).json({
        message: 'MantrixFlow API is running',
        docs: '/api/docs',
      });
    }
  });

  cachedApp = app;
  logger.log('Nest application initialized');
  return app;
}

async function bootstrap() {
  const app = await createApp();
  const configService = app.get(ConfigService);
  const logger = new Logger('Bootstrap');
  const port = configService.get<number>('PORT', 5000);
  await app.listen(port, '0.0.0.0');
  logger.log(`🚀 Application is running on: http://localhost:${port}`);
  logger.log(`📚 Swagger documentation: http://localhost:${port}/api/docs`);
}

function isServerless(): boolean {
  return !!(
    process.env.VERCEL ||
    process.env.AWS_LAMBDA_FUNCTION_NAME ||
    process.env.LAMBDA_TASK_ROOT
  );
}

/**
 * Serverless handler for Vercel / AWS Lambda.
 * Exported so the runtime finds "exports" and does not throw "No exports found in module".
 */
async function handler(req: Request, res: Response): Promise<void> {
  const app = await createApp();
  const expressInstance = app.getHttpAdapter().getInstance() as Application;
  expressInstance(req, res);
}

export default handler;
export { handler };

// When run directly (e.g. node dist/main.js or bun dist/main), start HTTP server
if (!isServerless() && require.main === module) {
  bootstrap().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
