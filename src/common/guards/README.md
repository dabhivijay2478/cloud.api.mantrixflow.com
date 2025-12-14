# Supabase Authentication Guard

This guard verifies Supabase JWT tokens and extracts user information from authenticated requests.

## Setup

### Environment Variables

Add these to your `.env` file:

```env
SUPABASE_URL=your_supabase_project_url
SUPABASE_ANON_KEY=your_supabase_anon_key
```

### Usage

Apply the guard to controllers or routes:

```typescript
import { UseGuards } from '@nestjs/common';
import { SupabaseAuthGuard } from '../../common/guards/supabase-auth.guard';

@UseGuards(SupabaseAuthGuard)
@Controller('api/your-endpoint')
export class YourController {
  // ...
}
```

## How It Works

1. Extracts the Bearer token from the `Authorization` header
2. Verifies the token with Supabase using `getUser()`
3. Attaches user information to the request object:
   - `req.user.id` - User ID from Supabase
   - `req.user.email` - User email
   - `req.user.orgId` - Organization ID (from user metadata if available)

## Accessing User Info

In your controller methods:

```typescript
@Get()
async getData(@Request() req: Request) {
  const userId = req.user?.id;
  const orgId = req.user?.orgId;
  // Use user info...
}
```

## Error Handling

The guard throws `UnauthorizedException` if:
- No token is provided
- Token is invalid or expired
- Token verification fails
