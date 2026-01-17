
ALTER TABLE "pipeline_runs" ADD COLUMN "resolved_destination_schema" varchar(255);--> statement-breakpoint
ALTER TABLE "pipeline_runs" ADD COLUMN "resolved_destination_table" varchar(255);--> statement-breakpoint
ALTER TABLE "pipeline_runs" ADD COLUMN "resolved_column_mappings" jsonb;--> statement-breakpoint
ALTER TABLE "pipeline_runs" ADD COLUMN "destination_table_was_created" varchar(10);--> statement-breakpoint
ALTER TABLE "pipeline_runs" ADD COLUMN "last_sync_cursor" text;--> statement-breakpoint
ALTER TABLE "pipeline_runs" ADD COLUMN "job_state_updated_at" timestamp;--> statement-breakpoint