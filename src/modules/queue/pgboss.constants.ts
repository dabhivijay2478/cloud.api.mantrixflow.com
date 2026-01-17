/**
 * PgBoss Constants
 * Injection tokens and queue names
 */

// Injection token for PgBoss options
export const PGBOSS_OPTIONS = Symbol('PGBOSS_OPTIONS');

// Queue names - centralized for consistency
export const QUEUE_NAMES = {
  // Data pipeline queues
  PIPELINE_EXECUTION: 'pipeline:execution',
  PIPELINE_SCHEDULED: 'pipeline:scheduled',
  
  // Email/notification queues
  NOTIFICATIONS: 'notifications',
  EMAILS: 'emails',
  
  // Data processing queues
  DATA_SYNC: 'data:sync',
  DATA_TRANSFORM: 'data:transform',
  DATA_EXPORT: 'data:export',
  
  // System queues
  CLEANUP: 'system:cleanup',
  ANALYTICS: 'system:analytics',
  AUDIT_LOG: 'system:audit',
  
  // Webhook queues
  WEBHOOKS: 'webhooks',
  WEBHOOK_RETRY: 'webhooks:retry',
} as const;

export type QueueName = typeof QUEUE_NAMES[keyof typeof QUEUE_NAMES];

// Job priorities
export const JOB_PRIORITY = {
  LOW: 10,
  NORMAL: 0,
  HIGH: -5,
  CRITICAL: -10,
} as const;

// Default job options
export const DEFAULT_JOB_OPTIONS = {
  retryLimit: 3,
  retryDelay: 60, // 60 seconds
  retryBackoff: true,
  expireInHours: 24,
} as const;
