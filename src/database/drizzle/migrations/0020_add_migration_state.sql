-- Add migration_state column to pipelines (schema has it but no prior migration)
ALTER TABLE pipelines ADD COLUMN IF NOT EXISTS migration_state varchar(50);
