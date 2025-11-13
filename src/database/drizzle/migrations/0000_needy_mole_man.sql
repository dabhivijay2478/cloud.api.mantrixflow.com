CREATE TYPE "public"."connection_status" AS ENUM('active', 'inactive', 'error');--> statement-breakpoint
CREATE TYPE "public"."query_log_status" AS ENUM('success', 'error');--> statement-breakpoint
CREATE TYPE "public"."sync_frequency" AS ENUM('manual', '15min', '1hour', '24hours');--> statement-breakpoint
CREATE TYPE "public"."sync_job_status" AS ENUM('pending', 'running', 'success', 'failed');--> statement-breakpoint
CREATE TYPE "public"."sync_mode" AS ENUM('full', 'incremental');--> statement-breakpoint
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
ALTER TABLE "postgres_query_logs" ADD CONSTRAINT "postgres_query_logs_connection_id_postgres_connections_id_fk" FOREIGN KEY ("connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
ALTER TABLE "postgres_sync_jobs" ADD CONSTRAINT "postgres_sync_jobs_connection_id_postgres_connections_id_fk" FOREIGN KEY ("connection_id") REFERENCES "public"."postgres_connections"("id") ON DELETE cascade ON UPDATE no action;