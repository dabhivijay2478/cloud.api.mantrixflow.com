# User Management Module

## Overview

This module handles user management, syncing users from Supabase Auth to the backend database, and managing user metadata.

## Database Schema

### Users Table

The `users` table stores user information synced from Supabase:

- `id` (UUID, Primary Key) - Supabase user ID
- `email` (VARCHAR, Unique) - User email
- `firstName`, `lastName`, `fullName` - User name fields
- `avatarUrl` - User avatar URL
- `supabaseUserId` (Unique) - Supabase user ID (duplicate for querying)
- `emailVerified` - Email verification status
- `metadata` (JSONB) - Additional user metadata
- `status` - User status (active, inactive, suspended)
- `currentOrgId` - Currently active organization
- `onboardingCompleted` - Onboarding completion status
- `onboardingStep` - Current onboarding step
- `lastLoginAt` - Last login timestamp
- `createdAt`, `updatedAt` - Timestamps

## Migration

To create the users table, run:

```bash
cd apps/api
bun run db:generate
bun run db:migrate
```

## API Endpoints

### User Management

- `POST /api/users/sync` - Sync user from Supabase
- `GET /api/users/me` - Get current user
- `GET /api/users/:id` - Get user by ID
- `PATCH /api/users/me` - Update current user
- `PATCH /api/users/me/onboarding` - Update onboarding status

### Webhooks

- `POST /api/webhooks/supabase/user` - Supabase auth webhook (for automatic user creation)

## Automatic User Creation

Users are automatically created/synced in two ways:

1. **After Login/Signup**: The `loginAction` and `signupAction` in the frontend automatically sync users
2. **After Email Confirmation**: The auth callback route syncs users
3. **Via Webhook**: Supabase can call the webhook endpoint when users are created/updated

## Environment Variables

For webhook security (optional but recommended):

```env
SUPABASE_WEBHOOK_SECRET=your_webhook_secret_here
```

## Usage

### Frontend

```tsx
import { useCurrentUser, useSyncUser } from '@/lib/api';

function MyComponent() {
  const { data: user } = useCurrentUser();
  const syncUser = useSyncUser();
  
  // Sync user after auth
  await syncUser.mutateAsync({
    supabaseUserId: supabaseUser.id,
    email: supabaseUser.email,
    // ...
  });
}
```

### Backend

```typescript
// User is automatically synced after Supabase auth
// Access user in controllers via req.user
@UseGuards(SupabaseAuthGuard)
@Get('me')
async getCurrentUser(@Request() req: Request) {
  const userId = req.user?.id;
  const user = await this.userService.getUserById(userId);
  return user;
}
```
