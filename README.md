# README.md

## Intro
`go-alertme` (previously `mk-alert`) is a WhatsApp-based notification program written in Go. It reads
alert rule settings from DB, then check the criteria against current state in
database MediaKernels.

## Alert Scopes

1. Project: the defined alert rule (code, operator, threshold, recipients) will
   be applied to given project ID only.
2. Global: the defined alert rule will be applied to all active (status =
   'running') projects such that no duplicates of alert_code & project_id
   combination is violated.
3. Misc: some alert codes do not apply to any project_id at all due to their
   natures. In this case, project_id will be set to 0. Currently the alert code
   is `min_twitter_doc_per_hour` and `max_twitter_doc_per_hour`. It counts
   documents directly to `Solr` and does not involve project at all.
   
## Design Consideration

Since several global rules will make alert setting grows to perhaps tens of
thousands of alert rules, using concurrent pattern based on `goroutines` passing
around channels containing alert rule is not practical. It quickly consumes
memory and keeps growing until our 256 GB machine saturated. It is still the
case even if we pass the reference to channel instead of its original data.
Other way that we have tried is combining channel of empty `struct` (which
should mean zero size, right?) and `Redis` queue but the problem persists. The
memory consumptions keeps growing albeit more slowly, and the end result is no
different.

Current method that works for us is using sequential process up until inserting
rules to `Redis` queue then concurrently consume it via pre-defined number of
parallel `goroutines`. All running `goroutines` is synchronized via
`sync.WaitGroup` mechanism such that no more than desired number of `goroutines`
running at the same time, and another one will be launched as soon as all of
them finished.

## How It Works

First, it will read WA session from `/tmp/whatsappSession.gob` to login. If
failed, it will prompt QR code for user to scan using the app in the mobile
devices. It will initiate `Redis` and database connection.

Afterward, it will read table `alert_rule_setting` to get its alert code,
threshold, recipients, and the project ID it should apply the alert to. If the
scope is "project", the entry is read as is. If the scope is "global", then it
loops over active project from table "project" and add project ID entry for
given metrics. This is done such that there will be unique combination of
alert_code and project_id (no duplicates) to prevent annoying recipients.
All settings are stored in `Redis` queue. This is done via `pushAlertSetting`
function.


## Known Issues

1. One or two data race condition is still encountered
2. There are warnings of sql.QueryRow complaining 0 arguments is intended but
   have 2 instead; I consult the documentation and actually it should receive
   one argument in addition to `variadic` inputs (presumably the SQL parameters)
3. Some `wac.Send` errors despite successful sending of alert notifications.

## TODO
1. Recurring and recurring interval is not currently implemented. Alternative
   workaround: run through cron jobs for desired interval.
2. When in an unfortunate case alert rule conflict exists, pick one with highest
   threshold (for MAX operator) and lowest (for MIN operator), unless different
   recipients want different threshold for the same alert rule & project_id
   combinations.
