import {
  boolean,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
  varchar,
} from 'drizzle-orm/pg-core';

/**
 * Enum for user status
 */
export const userStatusEnum = pgEnum('user_status', ['active', 'inactive', 'suspended']);

/**
 * Users Table
 * Stores user information synced from Supabase Auth
 */
export const users = pgTable('users', {
  id: uuid('id').primaryKey(), // Supabase user ID
  email: varchar('email', { length: 255 }).notNull().unique(),
  firstName: varchar('first_name', { length: 100 }),
  lastName: varchar('last_name', { length: 100 }),
  fullName: varchar('full_name', { length: 200 }),
  avatarUrl: text('avatar_url'),
  // Supabase metadata
  supabaseUserId: varchar('supabase_user_id', { length: 255 }).notNull().unique(),
  emailVerified: boolean('email_verified').notNull().default(false),
  // User metadata
  metadata: jsonb('metadata'), // Additional user data
  // Status
  status: userStatusEnum('status').notNull().default('active'),
  // Current organization
  currentOrgId: uuid('current_org_id'),
  // Onboarding
  onboardingCompleted: boolean('onboarding_completed').notNull().default(false),
  onboardingStep: varchar('onboarding_step', { length: 50 }),
  onboardingNudge1SentAt: timestamp('onboarding_nudge_1_sent_at', { withTimezone: true }),
  onboardingNudge2SentAt: timestamp('onboarding_nudge_2_sent_at', { withTimezone: true }),
  // Timestamps
  lastLoginAt: timestamp('last_login_at'),
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type User = typeof users.$inferSelect;
export type NewUser = typeof users.$inferInsert;
