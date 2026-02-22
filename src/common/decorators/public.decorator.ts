import { SetMetadata } from '@nestjs/common';

export const IS_PUBLIC_KEY = 'isPublic';

/**
 * Marks a route as public — skips SupabaseAuthGuard.
 * Use for connector metadata, health checks, etc.
 */
export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);
