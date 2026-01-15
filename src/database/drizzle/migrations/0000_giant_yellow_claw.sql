CREATE TYPE "public"."user_status" AS ENUM('active', 'inactive', 'suspended');--> statement-breakpoint
CREATE TYPE "public"."organization_member_role" AS ENUM('OWNER', 'ADMIN', 'EDITOR', 'VIEWER');--> statement-breakpoint
CREATE TYPE "public"."organization_member_status" AS ENUM('invited', 'accepted', 'active', 'inactive');--> statement-breakpoint
CREATE TYPE "public"."subscription_plan" AS ENUM('basic', 'pro', 'enterprise');--> statement-breakpoint
CREATE TYPE "public"."subscription_status" AS ENUM('active', 'on_hold', 'failed', 'canceled', 'trialing');--> statement-breakpoint
CREATE TYPE "public"."subscription_event_type" AS ENUM('payment.succeeded', 'payment.failed', 'payment.processing', 'payment.cancelled', 'subscription.created', 'subscription.active', 'subscription.activated', 'subscription.updated', 'subscription.on_hold', 'subscription.renewed', 'subscription.canceled', 'subscription.failed', 'subscription.trial_started', 'subscription.trial_ended');--> statement-breakpoint
CREATE TYPE "public"."connection_status" AS ENUM('active', 'inactive', 'error');--> statement-breakpoint
CREATE TYPE "public"."query_log_status" AS ENUM('success', 'error');--> statement-breakpoint
CREATE TYPE "public"."sync_frequency" AS ENUM('manual', '15min', '1hour', '24hours');--> statement-breakpoint
CREATE TYPE "public"."sync_job_status" AS ENUM('pending', 'running', 'success', 'failed');--> statement-breakpoint
CREATE TYPE "public"."sync_mode" AS ENUM('full', 'incremental');--> statement-breakpoint
CREATE TYPE "public"."migration_state" AS ENUM('pending', 'running', 'listing');--> statement-breakpoint
CREATE TYPE "public"."pipeline_status" AS ENUM('active', 'paused', 'error');--> statement-breakpoint
CREATE TYPE "public"."run_status" AS ENUM('pending', 'running', 'success', 'failed', 'cancelled');--> statement-breakpoint
CREATE TYPE "public"."write_mode" AS ENUM('append', 'upsert', 'replace');--> statement-breakpoint
CREATE TYPE "public"."job_state" AS ENUM('pending', 'setup', 'running', 'paused', 'listing', 'stopped', 'completed', 'error');--> statement-breakpoint
CREATE TYPE "public"."trigger_type" AS ENUM('manual', 'scheduled', 'webhook');--> statement-breakpoint
CREATE TABLE "users" (
	"id" uuid PRIMARY KEY NOT NULL,
	"email" varchar(255) NOT NULL,
	"first_name" varchar(100),
	"last_name" varchar(100),
	"full_name" varchar(200),
	"avatar_url" text,
	"supabase_user_id" varchar(255) NOT NULL,
	"email_verified" boolean DEFAULT false NOT NULL,
	"metadata" jsonb,
	"status" "user_status" DEFAULT 'active' NOT NULL,
	"current_org_id" uuid,
	"onboarding_completed" boolean DEFAULT false NOT NULL,
	"onboarding_step" varchar(50),
	"last_login_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "users_email_unique" UNIQUE("email"),
	CONSTRAINT "users_supabase_user_id_unique" UNIQUE("supabase_user_id")
);
--> statement-breakpoint
CREATE TABLE "organizations" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"name" varchar(255) NOT NULL,
	"slug" varchar(255) NOT NULL,
	"description" text,
	"owner_user_id" uuid,
	"metadata" jsonb,
	"settings" jsonb,
	"is_active" boolean DEFAULT true NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "organizations_slug_unique" UNIQUE("slug")
);
--> statement-breakpoint
CREATE TABLE "organization_members" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid,
	"email" varchar(255) NOT NULL,
	"role" "organization_member_role" DEFAULT 'VIEWER' NOT NULL,
	"status" "organization_member_status" DEFAULT 'invited' NOT NULL,
	"invited_by" uuid,
	"invited_at" timestamp DEFAULT now() NOT NULL,
	"accepted_at" timestamp,
	"agent_panel_access" boolean DEFAULT false NOT NULL,
	"allowed_models" jsonb DEFAULT '[]'::jsonb,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "organization_owners" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "dodo_subscriptions" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"user_id" uuid NOT NULL,
	"plan_id" "subscription_plan" NOT NULL,
	"status" "subscription_status" DEFAULT 'trialing' NOT NULL,
	"current_period_start" timestamp,
	"current_period_end" timestamp,
	"trial_start" timestamp,
	"trial_end" timestamp,
	"canceled_at" timestamp,
	"cancel_at_period_end" timestamp,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "dodo_subscriptions_user_id_unique" UNIQUE("user_id")
);
--> statement-breakpoint
CREATE TABLE "dodo_subscription_events" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"subscription_id" uuid NOT NULL,
	"event_type" "subscription_event_type" NOT NULL,
	"dodo_event_id" varchar(255),
	"payload" jsonb NOT NULL,
	"processed" timestamp DEFAULT now(),
	"error" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "dodo_subscription_events_dodo_event_id_unique" UNIQUE("dodo_event_id")
);
--> statement-breakpoint
CREATE TABLE "dodo_customers" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"user_id" uuid NOT NULL,
	"dodo_customer_id" varchar(255) NOT NULL,
	"subscription_id" uuid,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "dodo_customers_user_id_unique" UNIQUE("user_id"),
	CONSTRAINT "dodo_customers_dodo_customer_id_unique" UNIQUE("dodo_customer_id")
);
--> statement-breakpoint
CREATE TABLE "activity_logs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"organization_id" uuid NOT NULL,
	"user_id" uuid,
	"action_type" text NOT NULL,
	"entity_type" text NOT NULL,
	"entity_id" uuid,
	"message" text NOT NULL,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "postgres_connections" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	"host" text NOT NULL,
	"port" integer DEFAULT 5432 NOT NULL,
	"database" text NOT NULL,
	"username" text NOT NULL,
	"password" text NOT NULL,
	"ssl_enabled" boolean DEFAULT false NOT NULL,
	"ssl_ca_cert" text,
	"ssh_tunnel_enabled" boolean DEFAULT false NOT NULL,
	"ssh_host" text,
	"ssh_port" integer,
	"ssh_username" text,
	"ssh_private_key" text,
	"connection_pool_size" integer DEFAULT 5 NOT NULL,
	"query_timeout_seconds" integer DEFAULT 60 NOT NULL,
	"status" "connection_status" DEFAULT 'inactive' NOT NULL,
	"last_connected_at" timestamp,
	"last_error" text,
	"schema_cache" jsonb,
	"schema_cached_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "postgres_query_logs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"connection_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"query" text NOT NULL,
	"execution_time_ms" integer NOT NULL,
	"rows_returned" integer DEFAULT 0 NOT NULL,
	"status" "query_log_status" NOT NULL,
	"error_message" text,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "postgres_sync_jobs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"connection_id" uuid NOT NULL,
	"table_name" varchar(255) NOT NULL,
	"sync_mode" "sync_mode" NOT NULL,
	"incremental_column" varchar(255),
	"last_sync_value" text,
	"destination_table" varchar(255) NOT NULL,
	"status" "sync_job_status" DEFAULT 'pending' NOT NULL,
	"rows_synced" integer DEFAULT 0 NOT NULL,
	"started_at" timestamp,
	"completed_at" timestamp,
	"error_message" text,
	"sync_frequency" "sync_frequency" DEFAULT 'manual' NOT NULL,
	"next_sync_at" timestamp,
	"custom_where_clause" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "pipeline_source_schemas" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"source_type" varchar(100) NOT NULL,
	"source_connection_id" uuid,
	"source_config" jsonb,
	"source_schema" varchar(255),
	"source_table" varchar(255),
	"source_query" text,
	"discovered_columns" jsonb,
	"primary_keys" jsonb,
	"foreign_keys" jsonb,
	"estimated_row_count" jsonb,
	"size_mb" jsonb,
	"validation_result" jsonb,
	"name" varchar(255),
	"is_active" boolean DEFAULT true,
	"last_discovered_at" timestamp,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "pipeline_destination_schemas" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"destination_connection_id" uuid NOT NULL,
	"destination_schema" varchar(255) DEFAULT 'public',
	"destination_table" varchar(255) NOT NULL,
	"destination_table_exists" boolean DEFAULT false,
	"column_definitions" jsonb,
	"primary_keys" jsonb,
	"indexes" jsonb,
	"column_mappings" jsonb,
	"write_mode" varchar(50) DEFAULT 'append',
	"upsert_key" jsonb,
	"validation_result" jsonb,
	"last_validated_at" timestamp,
	"last_synced_at" timestamp,
	"name" varchar(255),
	"is_active" boolean DEFAULT true,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "postgres_pipelines" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	"description" text,
	"source_type" varchar(100),
	"source_schema_id" uuid NOT NULL,
	"destination_schema_id" uuid NOT NULL,
	"destination_connection_id" uuid NOT NULL,
	"destination_table" varchar(255),
	"transformations" jsonb,
	"sync_mode" varchar(50) DEFAULT 'full',
	"incremental_column" varchar(255),
	"last_sync_value" text,
	"sync_frequency" varchar(50) DEFAULT 'manual',
	"next_sync_at" timestamp,
	"status" "pipeline_status" DEFAULT 'active',
	"last_run_at" timestamp,
	"last_run_status" "run_status",
	"last_error" text,
	"migration_state" "migration_state" DEFAULT 'pending',
	"total_rows_processed" integer DEFAULT 0,
	"total_runs_successful" integer DEFAULT 0,
	"total_runs_failed" integer DEFAULT 0,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);
--> statement-breakpoint
CREATE TABLE "postgres_pipeline_runs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"pipeline_id" uuid NOT NULL,
	"org_id" uuid NOT NULL,
	"status" "run_status" DEFAULT 'pending',
	"resolved_destination_schema" varchar(255),
	"resolved_destination_table" varchar(255),
	"destination_table_was_created" varchar(10),
	"resolved_column_mappings" jsonb,
	"job_state" "job_state" DEFAULT 'pending',
	"last_sync_cursor" text,
	"job_state_updated_at" timestamp,
	"rows_read" integer DEFAULT 0,
	"rows_written" integer DEFAULT 0,
	"rows_skipped" integer DEFAULT 0,
	"rows_failed" integer DEFAULT 0,
	"bytes_processed" integer DEFAULT 0,
	"started_at" timestamp,
	"completed_at" timestamp,
	"duration_seconds" integer,
	"error_message" text,
	"error_code" varchar(50),
	"error_stack" text,
	"trigger_type" "trigger_type" DEFAULT 'manual',
	"triggered_by" uuid,
	"run_metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
ALTER TABLE "organizations" ADD CONSTRAINT "organizations_owner_user_id_users_id_fk" FOREIGN KEY ("owner_user_id") REFERENCES "public"."users"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "organization_members" ADD CONSTRAINT "organization_members_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "organization_members" ADD CONSTRAINT "organization_members_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "organization_members" ADD CONSTRAINT "organization_members_invited_by_users_id_fk" FOREIGN KEY ("invited_by") REFERENCES "public"."users"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "organization_owners" ADD CONSTRAINT "organization_owners_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "organization_owners" ADD CONSTRAINT "organization_owners_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "dodo_subscriptions" ADD CONSTRAINT "dodo_subscriptions_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "dodo_subscription_events" ADD CONSTRAINT "dodo_subscription_events_subscription_id_dodo_subscriptions_id_fk" FOREIGN KEY ("subscription_id") REFERENCES "public"."dodo_subscriptions"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "dodo_customers" ADD CONSTRAINT "dodo_customers_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "dodo_customers" ADD CONSTRAINT "dodo_customers_subscription_id_dodo_subscriptions_id_fk" FOREIGN KEY ("subscription_id") REFERENCES "public"."dodo_subscriptions"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "activity_logs" ADD CONSTRAINT "activity_logs_organization_id_organizations_id_fk" FOREIGN KEY ("organization_id") REFERENCES "public"."organizations"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "activity_logs" ADD CONSTRAINT "activity_logs_user_id_users_id_fk" FOREIGN KEY ("user_id") REFERENCES "public"."users"("id") ON DELETE set null ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_query_logs" ADD CONSTRAINT "postgres_query_logs_connection_id_postgres_connections_id_fk" FOREIGN KEY ("connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_sync_jobs" ADD CONSTRAINT "postgres_sync_jobs_connection_id_postgres_connections_id_fk" FOREIGN KEY ("connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "pipeline_source_schemas" ADD CONSTRAINT "pipeline_source_schemas_source_connection_id_postgres_connections_id_fk" FOREIGN KEY ("source_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "pipeline_destination_schemas" ADD CONSTRAINT "pipeline_destination_schemas_destination_connection_id_postgres_connections_id_fk" FOREIGN KEY ("destination_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_source_schema_id_pipeline_source_schemas_id_fk" FOREIGN KEY ("source_schema_id") REFERENCES "public"."pipeline_source_schemas"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_destination_schema_id_pipeline_destination_schemas_id_fk" FOREIGN KEY ("destination_schema_id") REFERENCES "public"."pipeline_destination_schemas"("id") ON DELETE restrict ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_pipeline_runs" ADD CONSTRAINT "postgres_pipeline_runs_pipeline_id_postgres_pipelines_id_fk" FOREIGN KEY ("pipeline_id") REFERENCES "public"."postgres_pipelines"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE INDEX "activity_logs_organization_id_idx" ON "activity_logs" USING btree ("organization_id");--> statement-breakpoint
CREATE INDEX "activity_logs_org_entity_type_idx" ON "activity_logs" USING btree ("organization_id","entity_type");--> statement-breakpoint
CREATE INDEX "activity_logs_org_action_type_idx" ON "activity_logs" USING btree ("organization_id","action_type");--> statement-breakpoint
CREATE INDEX "activity_logs_user_id_idx" ON "activity_logs" USING btree ("user_id");--> statement-breakpoint
CREATE INDEX "activity_logs_entity_idx" ON "activity_logs" USING btree ("entity_type","entity_id");--> statement-breakpoint
CREATE INDEX "activity_logs_created_at_idx" ON "activity_logs" USING btree ("created_at");