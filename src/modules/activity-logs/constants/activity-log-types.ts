/**
 * Activity Log Types
 * Centralized constants for all activity log action types
 *
 * These constants are used throughout the application to ensure
 * consistent logging and avoid magic strings.
 */

/**
 * Organization Action Types
 */
export const ORG_ACTIONS = {
  CREATED: 'ORG_CREATED',
  UPDATED: 'ORG_UPDATED',
  SELECTED: 'ORG_SELECTED',
  DELETED: 'ORG_DELETED',
} as const;

/**
 * User & Member Action Types
 */
export const USER_ACTIONS = {
  INVITED: 'USER_INVITED',
  INVITE_ACCEPTED: 'USER_INVITE_ACCEPTED',
  ROLE_CHANGED: 'USER_ROLE_CHANGED',
  REMOVED: 'USER_REMOVED',
} as const;

/**
 * Pipeline Action Types
 */
export const PIPELINE_ACTIONS = {
  CREATED: 'PIPELINE_CREATED',
  UPDATED: 'PIPELINE_UPDATED',
  PUBLISHED: 'PIPELINE_PUBLISHED',
  RUN_STARTED: 'PIPELINE_RUN_STARTED',
  RUN_PAUSED: 'PIPELINE_RUN_PAUSED',
  RUN_RESUMED: 'PIPELINE_RUN_RESUMED',
  RUN_STOPPED: 'PIPELINE_RUN_STOPPED',
  RUN_FAILED: 'PIPELINE_RUN_FAILED',
  RUN_SUCCEEDED: 'PIPELINE_RUN_SUCCEEDED',
  CONFIGURATION_CHANGED: 'PIPELINE_CONFIGURATION_CHANGED',
} as const;

/**
 * Migration Action Types
 */
export const MIGRATION_ACTIONS = {
  STARTED: 'MIGRATION_STARTED',
  COMPLETED: 'MIGRATION_COMPLETED',
  FAILED: 'MIGRATION_FAILED',
  INCREMENTAL_SYNC_EXECUTED: 'MIGRATION_INCREMENTAL_SYNC_EXECUTED',
  RECORDS_INSERTED: 'MIGRATION_RECORDS_INSERTED',
  RECORDS_UPDATED: 'MIGRATION_RECORDS_UPDATED',
} as const;

/**
 * Data Source Action Types
 */
export const DATASOURCE_ACTIONS = {
  CONNECTED: 'DATASOURCE_CONNECTED',
  UPDATED: 'DATASOURCE_UPDATED',
  DELETED: 'DATASOURCE_DELETED',
  SCHEMA_VALIDATION_FAILED: 'DATASOURCE_SCHEMA_VALIDATION_FAILED',
} as const;

/**
 * Destination Action Types
 */
export const DESTINATION_ACTIONS = {
  CONNECTED: 'DESTINATION_CONNECTED',
  UPDATED: 'DESTINATION_UPDATED',
  DELETED: 'DESTINATION_DELETED',
  UPSERT_CONSTRAINT_MISSING: 'DESTINATION_UPSERT_CONSTRAINT_MISSING',
} as const;

/**
 * Transformation & Mapping Action Types
 */
export const MAPPING_ACTIONS = {
  FIELD_MAPPING_CREATED: 'FIELD_MAPPING_CREATED',
  FIELD_MAPPING_UPDATED: 'FIELD_MAPPING_UPDATED',
  FIELD_MAPPING_DELETED: 'FIELD_MAPPING_DELETED',
  TRANSFORMATION_VALIDATION_FAILED: 'TRANSFORMATION_VALIDATION_FAILED',
  TRANSFORMATION_VALIDATION_SUCCEEDED: 'TRANSFORMATION_VALIDATION_SUCCEEDED',
} as const;

/**
 * Entity Types
 */
export const ENTITY_TYPES = {
  ORGANIZATION: 'organization',
  PIPELINE: 'pipeline',
  MIGRATION: 'migration',
  DATASOURCE: 'datasource',
  DESTINATION: 'destination',
  USER: 'user',
  MAPPING: 'mapping',
} as const;

/**
 * All action types combined
 */
export const ACTIVITY_LOG_ACTIONS = {
  ...ORG_ACTIONS,
  ...USER_ACTIONS,
  ...PIPELINE_ACTIONS,
  ...MIGRATION_ACTIONS,
  ...DATASOURCE_ACTIONS,
  ...DESTINATION_ACTIONS,
  ...MAPPING_ACTIONS,
} as const;

/**
 * Type for action type values
 */
export type ActivityLogActionType =
  | (typeof ORG_ACTIONS)[keyof typeof ORG_ACTIONS]
  | (typeof USER_ACTIONS)[keyof typeof USER_ACTIONS]
  | (typeof PIPELINE_ACTIONS)[keyof typeof PIPELINE_ACTIONS]
  | (typeof MIGRATION_ACTIONS)[keyof typeof MIGRATION_ACTIONS]
  | (typeof DATASOURCE_ACTIONS)[keyof typeof DATASOURCE_ACTIONS]
  | (typeof DESTINATION_ACTIONS)[keyof typeof DESTINATION_ACTIONS]
  | (typeof MAPPING_ACTIONS)[keyof typeof MAPPING_ACTIONS];

/**
 * Type for entity type values
 */
export type ActivityLogEntityType = (typeof ENTITY_TYPES)[keyof typeof ENTITY_TYPES];
