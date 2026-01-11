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
 * AUTHORITATIVE ROLES:
 * - OWNER: Exactly ONE per organization. Full control including org settings, user management, and all data.
 * - ADMIN: Can manage workspace data, data sources, pipelines, and users. Cannot update org details or change ownership.
 * - EDITOR: Can edit workspace data, manage data sources and pipelines. Cannot manage users or org settings.
 * - VIEWER: Read-only access. Can view all data but cannot edit anything.
 */
export const organizationMemberRoleEnum = pgEnum('organization_member_role', [
  'OWNER',
  'ADMIN',
  'EDITOR',
  'VIEWER',
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
  // AUTHORITATIVE: Must be one of OWNER, ADMIN, EDITOR, VIEWER
  // OWNER role is enforced to be unique per organization (see migration)
  role: organizationMemberRoleEnum('role').notNull().default('VIEWER'),
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
