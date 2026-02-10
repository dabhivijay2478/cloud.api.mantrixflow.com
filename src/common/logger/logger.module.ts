/**
 * Logger Module
 * Global Pino-based logging for the entire NestJS application.
 *
 * - Replaces the built-in NestJS Logger with Pino (structured JSON)
 * - Pretty-prints in development, raw JSON in production
 * - LOG_LEVEL configurable via .env (default: info)
 * - Exposes ActivityLoggerService for structured activity tracking
 */

import { Global, Module } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { LoggerModule as PinoLoggerModule } from 'nestjs-pino';
import { ActivityLoggerService } from './activity-logger.service';

@Global()
@Module({
  imports: [
    PinoLoggerModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const isProduction = config.get<string>('NODE_ENV') === 'production';
        const logLevel = config.get<string>('LOG_LEVEL', 'info');

        return {
          pinoHttp: {
            level: logLevel,
            // In production emit raw JSON; in dev use pino-pretty for readability
            ...(isProduction
              ? {}
              : {
                  transport: {
                    target: 'pino-pretty',
                    options: {
                      colorize: true,
                      singleLine: false,
                      translateTime: 'SYS:yyyy-mm-dd HH:MM:ss.l',
                      ignore: 'pid,hostname',
                    },
                  },
                }),
            // Serialise request/response for HTTP logs
            serializers: {
              req: (req: any) => ({
                method: req.method,
                url: req.url,
                query: req.query,
                params: req.params,
              }),
              res: (res: any) => ({
                statusCode: res.statusCode,
              }),
            },
            // Custom log attributes attached to every HTTP log line
            customProps: () => ({
              service: 'nestjs-api',
            }),
          },
        };
      },
    }),
  ],
  providers: [ActivityLoggerService],
  exports: [ActivityLoggerService],
})
export class AppLoggerModule {}
