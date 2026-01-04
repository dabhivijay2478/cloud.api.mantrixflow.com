import { boolean, jsonb, pgEnum, pgTable, timestamp, uuid, varchar } from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';
import { organizations } from './organizations.schema';

/**
 * Enum for organization member status
 * - invited: User has been invited but hasn't signed up yet
 * - accepted: User has signed up and accepted the invite
 * - active: User is an active member of the organization
 * - inactive: User membership is inactive
 */
export const organizationMemberStatusEnum = pgEnum('organization_member_status', [
  'invited',
  'accepted',
  'active',
  'inactive',
]);

/**
 * Enum for organization member role
 */
export const organizationMemberRoleEnum = pgEnum('organization_member_role', [
  'owner',
  'admin',
  'member',
  'viewer',
  'guest',
]);

/**
 * Organization Members Table
 * Tracks the relationship between users and organizations, including invite status
 * This is the source of truth for organization membership and invites
 */
export const organizationMembers = pgTable('organization_members', {
  id: uuid('id').primaryKey().defaultRandom(),
  // Organization reference
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),
  // User reference (nullable for invited users who haven't signed up yet)
  userId: uuid('user_id').references(() => users.id, { onDelete: 'cascade' }),
  // Email (required for invites, may differ from user.email if user changes email)
  email: varchar('email', { length: 255 }).notNull(),
  // Role in the organization
  role: organizationMemberRoleEnum('role').notNull().default('member'),
  // Status: invited -> accepted -> active
  status: organizationMemberStatusEnum('status').notNull().default('invited'),
  // Invite tracking
  invitedBy: uuid('invited_by').references(() => users.id, {
    onDelete: 'set null',
  }),
  invitedAt: timestamp('invited_at').notNull().defaultNow(),
  // When user accepted the invite (signed up)
  acceptedAt: timestamp('accepted_at'),
  // Agent panel access and permissions
  agentPanelAccess: boolean('agent_panel_access').notNull().default(false),
  allowedModels: jsonb('allowed_models').$type<string[]>().default([]),
  // Additional metadata
  metadata: jsonb('metadata'),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type OrganizationMember = typeof organizationMembers.$inferSelect;
export type NewOrganizationMember = typeof organizationMembers.$inferInsert;
