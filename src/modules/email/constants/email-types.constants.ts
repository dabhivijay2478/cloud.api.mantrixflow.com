/**
 * Email type constants for MantrixFlow transactional emails
 * Template IDs are configured via env (UNOSEND_TEMPLATE_*) or UnoSend dashboard
 */

export const EMAIL_TYPES = {
  // Pipeline Lifecycle
  PIPELINE_RUN_FAILED: 'pipeline_run_failed',
  PIPELINE_RECOVERED: 'pipeline_recovered',
  PIPELINE_PARTIAL_SUCCESS: 'pipeline_partial_success',
  PIPELINE_DISABLED: 'pipeline_disabled',
  FIRST_SUCCESS: 'first_success',
  LOG_BASED_INITIAL_COMPLETE: 'log_based_initial_complete',
  LONG_RUNNING_ALERT: 'long_running_alert',

  // Connection Management
  CONNECTION_TEST_FAILED: 'connection_test_failed',
  CONNECTION_RESTORED: 'connection_restored',
  LOG_BASED_SETUP_COMPLETE: 'log_based_setup_complete',
  REPLICATION_SLOT_WARNING: 'replication_slot_warning',

  // Billing and Subscription
  TRIAL_STARTED: 'trial_started',
  TRIAL_ENDS_7_DAYS: 'trial_ends_7_days',
  TRIAL_ENDS_1_DAY: 'trial_ends_1_day',
  TRIAL_EXPIRED: 'trial_expired',
  SUBSCRIPTION_UPGRADED: 'subscription_upgraded',
  PAYMENT_FAILED: 'payment_failed',
  SUBSCRIPTION_CANCELLED: 'subscription_cancelled',
  USAGE_APPROACHING_LIMIT: 'usage_approaching_limit',

  // System and Security Alerts
  NEW_DEVICE_LOGIN: 'new_device_login',
  API_KEY_CREATED: 'api_key_created',
  API_KEY_DELETED: 'api_key_deleted',
  MEMBER_REMOVED: 'member_removed',
  WEEKLY_DIGEST: 'weekly_digest',

  // Engagement
  ONBOARDING_DAY3_NUDGE: 'onboarding_day3_nudge',
  ONBOARDING_DAY7_NUDGE: 'onboarding_day7_nudge',
} as const;

export type EmailType = (typeof EMAIL_TYPES)[keyof typeof EMAIL_TYPES];

/** Map email type to local template filename (for local HTML rendering) */
export const EMAIL_TYPE_TO_TEMPLATE: Partial<Record<EmailType, string>> = {
  [EMAIL_TYPES.PIPELINE_RUN_FAILED]: 'pipeline_run_failed.html',
  [EMAIL_TYPES.PIPELINE_RECOVERED]: 'pipeline_recovered.html',
  [EMAIL_TYPES.PIPELINE_DISABLED]: 'pipeline_disabled.html',
  [EMAIL_TYPES.FIRST_SUCCESS]: 'first_success.html',
  [EMAIL_TYPES.LOG_BASED_INITIAL_COMPLETE]: 'log_based_initial_complete.html',
  [EMAIL_TYPES.PIPELINE_PARTIAL_SUCCESS]: 'pipeline_partial_success.html',
  [EMAIL_TYPES.LOG_BASED_SETUP_COMPLETE]: 'log_based_setup_complete.html',
  [EMAIL_TYPES.MEMBER_REMOVED]: 'member_removed.html',
  [EMAIL_TYPES.TRIAL_STARTED]: 'trial_started.html',
  [EMAIL_TYPES.TRIAL_ENDS_7_DAYS]: 'trial_ends_7_days.html',
  [EMAIL_TYPES.TRIAL_ENDS_1_DAY]: 'trial_ends_1_day.html',
  [EMAIL_TYPES.TRIAL_EXPIRED]: 'trial_expired.html',
  [EMAIL_TYPES.PAYMENT_FAILED]: 'payment_failed.html',
  [EMAIL_TYPES.WEEKLY_DIGEST]: 'weekly_digest.html',
  [EMAIL_TYPES.ONBOARDING_DAY3_NUDGE]: 'onboarding_day3_nudge.html',
  [EMAIL_TYPES.ONBOARDING_DAY7_NUDGE]: 'onboarding_day7_nudge.html',
};
