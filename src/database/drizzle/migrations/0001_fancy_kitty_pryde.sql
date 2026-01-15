ALTER TABLE "dodo_subscriptions" ADD COLUMN "dodo_subscription_id" varchar(255);--> statement-breakpoint
ALTER TABLE "dodo_subscriptions" ADD COLUMN "dodo_customer_id" varchar(255);--> statement-breakpoint
ALTER TABLE "dodo_subscriptions" ADD CONSTRAINT "dodo_subscriptions_dodo_subscription_id_unique" UNIQUE("dodo_subscription_id");