/**
 * Internal Token Guard
 * Protects /internal/* endpoints with X-Internal-Token header
 */

import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Request } from 'express';

@Injectable()
export class InternalTokenGuard implements CanActivate {
  constructor(private readonly configService: ConfigService) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<Request>();
    const token = request.headers['x-internal-token'] as string | undefined;
    const expected = this.configService.get<string>('INTERNAL_TOKEN');

    if (!expected) {
      throw new UnauthorizedException('INTERNAL_TOKEN not configured');
    }
    if (!token || token !== expected) {
      throw new UnauthorizedException('Invalid or missing X-Internal-Token');
    }
    return true;
  }
}
