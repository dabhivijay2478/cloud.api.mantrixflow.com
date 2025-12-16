/**
 * Type declarations for Express Request
 * Extends Express Request to include user property from SupabaseAuthGuard
 */

import 'express';

declare global {
  namespace Express {
    interface Request {
      user?: {
        id: string;
        email?: string;
        orgId?: string;
      };
    }
  }
}

export {};
