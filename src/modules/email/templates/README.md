# Email Templates

All MantrixFlow transactional email templates live in this folder. Use them as source when creating templates in UnoSend.

## Logo

Templates use `{{logo_url}}` for the header logo. The EmailService injects this automatically. Default: UnoSend CDN (`https://www.unosend.co/cdn/53483ed3-39f0-4330-b156-27176282bdf4/1772965120678-em5heh.png`). Override via `UNOSEND_LOGO_URL` env.

## Theme

Styles match `apps/app/app/globals.css`:
- **Primary** (#00a859) — main buttons
- **Secondary** (#007fff) — secondary buttons
- **Destructive** (#ef4444) — payment failed, trial ends tomorrow
- **Muted** (#f4f4f5, #71717a) — stats, footer
- **Font** — Geist (fallback: system fonts)

## Template Files

| File | Email Type | Variables |
|------|------------|-----------|
| `pipeline_run_failed.html` | pipeline_run_failed | pipeline_name, source_stream, dest_table, error_message, started_at, failed_at, run_detail_url, edit_pipeline_url |
| `pipeline_recovered.html` | pipeline_recovered | pipeline_name, rows_upserted, duration_seconds, run_history_url |
| `pipeline_disabled.html` | pipeline_disabled | pipeline_name, failure_count, last_error_message, edit_pipeline_url, support_url |
| `first_success.html` | first_success | pipeline_name, rows_upserted, dest_table, duration_seconds, pipeline_url |
| `log_based_initial_complete.html` | log_based_initial_complete | pipeline_name, rows_upserted, dest_table, pipeline_url |
| `pipeline_partial_success.html` | pipeline_partial_success | pipeline_name, rows_upserted, timeout_seconds, run_detail_url |
| `log_based_setup_complete.html` | log_based_setup_complete | connection_name, create_pipeline_url |
| `member_removed.html` | member_removed | first_name, org_name, dashboard_url |
| `trial_started.html` | trial_started | first_name, org_name, trial_end_date, pricing_url, dashboard_url |
| `trial_ends_7_days.html` | trial_ends_7_days | org_name, trial_end_date, pipeline_count, connection_count, rows_synced_total, upgrade_url |
| `trial_ends_1_day.html` | trial_ends_1_day | org_name, trial_end_date, upgrade_url |
| `trial_expired.html` | trial_expired | org_name, paused_pipeline_count, upgrade_url |
| `payment_failed.html` | payment_failed | org_name, amount, retry_date, grace_period_end_date, billing_url |
| `weekly_digest.html` | weekly_digest | org_name, week_start_date, total_runs, success_rate, failed_runs, rows_synced, top_pipeline_name, analytics_url |
| `onboarding_day3_nudge.html` | onboarding_day3_nudge | first_name, dashboard_url |
| `onboarding_day7_nudge.html` | onboarding_day7_nudge | first_name, dashboard_url |

## UnoSend Setup

1. Create each template in UnoSend using the HTML from these files.
2. Set the corresponding `UNOSEND_TEMPLATE_*` env var (e.g. `UNOSEND_TEMPLATE_PIPELINE_RUN_FAILED`).
3. All templates receive `logo_url` automatically—no need to add it in UnoSend.
