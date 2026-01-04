CREATE TYPE "public"."pipeline_status" AS ENUM('active', 'paused', 'error');--> statement-breakpoint
CREATE TYPE "public"."run_status" AS ENUM('pending', 'running', 'success', 'failed', 'cancelled');--> statement-breakpoint
CREATE TYPE "public"."trigger_type" AS ENUM('manual', 'scheduled', 'webhook');--> statement-breakpoint
CREATE TYPE "public"."write_mode" AS ENUM('append', 'upsert', 'replace');--> statement-breakpoint
CREATE TABLE "postgres_pipeline_runs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"pipeline_id" uuid NOT NULL,
	"org_id" uuid NOT NULL,
	"status" "run_status" DEFAULT 'pending',
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
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "postgres_pipelines" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"org_id" uuid NOT NULL,
	"user_id" uuid NOT NULL,
	"name" varchar(255) NOT NULL,
	"description" text,
	"source_type" varchar(100) NOT NULL,
	"source_connection_id" uuid,
	"source_config" jsonb,
	"source_schema" varchar(255),
	"source_table" varchar(255),
	"source_query" text,
	"destination_connection_id" uuid NOT NULL,
	"destination_schema" varchar(255) DEFAULT 'public',
	"destination_table" varchar(255) NOT NULL,
	"destination_table_exists" boolean DEFAULT false,
	"column_mappings" jsonb,
	"transformations" jsonb,
	"write_mode" "write_mode" DEFAULT 'append',
	"upsert_key" jsonb,
	"sync_mode" varchar(50) DEFAULT 'full',
	"incremental_column" varchar(255),
	"last_sync_value" text,
	"sync_frequency" varchar(50) DEFAULT 'manual',
	"next_sync_at" timestamp,
	"status" "pipeline_status" DEFAULT 'active',
	"last_run_at" timestamp,
	"last_run_status" "run_status",
	"last_error" text,
	"total_rows_processed" integer DEFAULT 0,
	"total_runs_successful" integer DEFAULT 0,
	"total_runs_failed" integer DEFAULT 0,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	"deleted_at" timestamp
);
--> statement-breakpoint
ALTER TABLE "postgres_pipeline_runs" ADD CONSTRAINT "postgres_pipeline_runs_pipeline_id_postgres_pipelines_id_fk" FOREIGN KEY ("pipeline_id") REFERENCES "public"."postgres_pipelines"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_source_connection_id_postgres_connections_id_fk" FOREIGN KEY ("source_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_pipelines" ADD CONSTRAINT "postgres_pipelines_destination_connection_id_postgres_connections_id_fk" FOREIGN KEY ("destination_connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;