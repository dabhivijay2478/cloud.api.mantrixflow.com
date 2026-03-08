# Email Module

Transactional emails via UnoSend for MantrixFlow.

## Templates

All HTML templates live in `templates/`. Styled to match `apps/app/app/globals.css` (primary #00a859, secondary #007fff). Logo: UnoSend CDN by default; override with `UNOSEND_LOGO_URL`. See `templates/README.md` for the full list.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `UNOSEND_API_KEY` | UnoSend API key (required for sending) |
| `EMAIL_ENABLED` | Set to `false` to disable all emails (default: `true`) |
| `UNOSEND_FROM_DEFAULT` | Default sender address (e.g. `alerts@mantrixflow.com`) |
| `FRONTEND_URL` | Base URL for links in emails (e.g. `https://mantrixflow.com`) |

## Template IDs

Set template IDs via env for each email type, e.g.:

- `UNOSEND_TEMPLATE_PIPELINE_RUN_FAILED`
- `UNOSEND_TEMPLATE_PIPELINE_RECOVERED`
- `UNOSEND_TEMPLATE_PIPELINE_DISABLED`
- `UNOSEND_TEMPLATE_FIRST_SUCCESS`
- `UNOSEND_TEMPLATE_LOG_BASED_INITIAL_COMPLETE`
- `UNOSEND_TEMPLATE_PIPELINE_PARTIAL_SUCCESS`
- `UNOSEND_TEMPLATE_LOG_BASED_SETUP_COMPLETE`
- `UNOSEND_TEMPLATE_MEMBER_REMOVED`
- `UNOSEND_TEMPLATE_TRIAL_STARTED`
- `UNOSEND_TEMPLATE_TRIAL_ENDS_7_DAYS`
- `UNOSEND_TEMPLATE_TRIAL_ENDS_1_DAY`
- `UNOSEND_TEMPLATE_TRIAL_EXPIRED`
- `UNOSEND_TEMPLATE_WEEKLY_DIGEST`

Template file → env variable
Template file	Env variable
pipeline_run_failed.html	UNOSEND_TEMPLATE_PIPELINE_RUN_FAILED
pipeline_recovered.html	UNOSEND_TEMPLATE_PIPELINE_RECOVERED
pipeline_disabled.html	UNOSEND_TEMPLATE_PIPELINE_DISABLED
first_success.html	UNOSEND_TEMPLATE_FIRST_SUCCESS
log_based_initial_complete.html	UNOSEND_TEMPLATE_LOG_BASED_INITIAL_COMPLETE
pipeline_partial_success.html	UNOSEND_TEMPLATE_PIPELINE_PARTIAL_SUCCESS
log_based_setup_complete.html	UNOSEND_TEMPLATE_LOG_BASED_SETUP_COMPLETE
member_removed.html	UNOSEND_TEMPLATE_MEMBER_REMOVED
trial_started.html	UNOSEND_TEMPLATE_TRIAL_STARTED
trial_ends_7_days.html	UNOSEND_TEMPLATE_TRIAL_ENDS_7_DAYS
trial_ends_1_day.html	UNOSEND_TEMPLATE_TRIAL_ENDS_1_DAY
trial_expired.html	UNOSEND_TEMPLATE_TRIAL_EXPIRED
weekly_digest.html	UNOSEND_TEMPLATE_WEEKLY_DIGEST
Templates not in the README list
These templates exist but are not in the README env list:

Template file	Env variable
payment_failed.html	UNOSEND_TEMPLATE_PAYMENT_FAILED
onboarding_day3_nudge.html	UNOSEND_TEMPLATE_ONBOARDING_DAY3_NUDGE
onboarding_day7_nudge.html	UNOSEND_TEMPLATE_ONBOARDING_DAY7_NUDGE


## Webhooks

- `POST /api/webhooks/email/unosend` — UnoSend bounce/unsubscribe webhook
- `POST /api/webhooks/billing` — Billing provider webhook (stub)

## Cron Jobs

- **Trial emails**: Daily 10:00 UTC — trial_ends_7_days, trial_ends_1_day, trial_expired
- **Weekly digest**: Monday 9:00 UTC — pipeline health summary per org
