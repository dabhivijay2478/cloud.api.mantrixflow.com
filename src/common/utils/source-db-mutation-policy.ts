const TRUE_VALUES = new Set(['1', 'true', 'yes', 'on']);

export const SOURCE_DB_MUTATION_POLICY_MESSAGE =
  'CDC and LOG_BASED sync are disabled by policy because they can alter the client source database (for example, replication slots).';

export function areSourceDbMutationsAllowed(): boolean {
  const raw =
    process.env.ALLOW_SOURCE_DB_MUTATIONS_FOR_CDC ??
    process.env.ALLOW_SOURCE_DB_MUTATIONS ??
    'false';

  return TRUE_VALUES.has(raw.trim().toLowerCase());
}
