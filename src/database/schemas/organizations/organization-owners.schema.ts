import { pgTable, timestamp, uuid } from 'drizzle-orm/pg-core';
import { users } from '../users/users.schema';
import { organizations } from './organizations.schema';

/**
 * Organization Owners Table
 * Tracks ownership of organizations separately from membership
 * A user can own multiple organizations, and ownership is distinct from membership
 *
 * This table is the single source of truth for organization ownership.
 * Ownership grants full control over the organization, while membership grants access based on role.
 */
export const organizationOwners = pgTable('organization_owners', {
  id: uuid('id').primaryKey().defaultRandom(),
  // Organization reference
  organizationId: uuid('organization_id')
    .notNull()
    .references(() => organizations.id, { onDelete: 'cascade' }),
  // User reference - the owner of the organization
  userId: uuid('user_id')
    .notNull()
    .references(() => users.id, { onDelete: 'cascade' }),
  // Timestamps
  createdAt: timestamp('created_at').notNull().defaultNow(),
  updatedAt: timestamp('updated_at').notNull().defaultNow(),
});

// Type exports for TypeScript
export type OrganizationOwner = typeof organizationOwners.$inferSelect;
export type NewOrganizationOwner = typeof organizationOwners.$inferInsert;
