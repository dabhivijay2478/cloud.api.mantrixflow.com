/**
 * Mock Auth Guard for E2E/testing.
 * Bypasses Supabase verification and injects a test user.
 */
import { type CanActivate, type ExecutionContext, Injectable } from '@nestjs/common';
import type { Request } from 'express';

export const TEST_USER_ID = '11111111-1111-1111-1111-111111111111';
export const TEST_USER_EMAIL = 'test@ai-bi.test';

@Injectable()
export class MockAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<Request>();
    const token = this.extractTokenFromHeader(request);

    if (!token) {
      request.user = {
        id: TEST_USER_ID,
        email: TEST_USER_EMAIL,
      };
      return true;
    }

    request.user = {
      id: token === 'test-org-user' ? '22222222-2222-2222-2222-222222222222' : TEST_USER_ID,
      email: TEST_USER_EMAIL,
      orgId: token === 'test-org-user' ? '33333333-3333-3333-3333-333333333333' : undefined,
    };
    return true;
  }

  private extractTokenFromHeader(request: Request): string | null {
    const authHeader = request.headers.authorization;
    if (!authHeader) return null;
    const [type, token] = authHeader.split(' ');
    return type === 'Bearer' ? token : null;
  }
}
