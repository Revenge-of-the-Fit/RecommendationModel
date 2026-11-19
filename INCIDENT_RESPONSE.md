# Incident response plan

Owner: Sara Santini · Version: 1.0 · Last reviewed: 2026-11-19

## Severity and scope

An incident is any event that stops, degrades or exposes the service or its data: an outage, slow or wrong answers, a cost spike, a leaked credential, unauthorized access, exposure of user data, or a management request to take something down.


| Level | Examples                                                                                                        | Acknowledge within | Recover within |
| ----- | --------------------------------------------------------------------------------------------------------------- | ------------------ | -------------- |
| SEV1  | Gateway down or not ready; any confirmed security event (leaked secret, unauthorized access, user data exposed) | 1 hour             | 12 hours       |
| SEV2  | Slow or fallback-only answers; quality, drift or cost alert; storage warning                                    | 12 hours           | 2 working days |
| SEV3  | non-blocking or single-user problems, dashboard glitches                                                        | Next working day   | As scheduled   |


- Data loss: only the event store is irreplaceable. Automatic backups are off in production (the storage-maintenance service runs with `--no-backups`). Manual backups are required before major changes and are recommended every week.
- If you cannot tell whether something is an outage or a security event, treat it as SEV1 until the incident lead decides otherwise.



## Detection

- **Automated:** the Grafana alert **Gateway unreachable** posts to the team Slack channel with `<!channel>` when the public gateway's `/health/ready` probe fails for 2 minutes (or no probe data arrives). The message names the trigger and links this plan and the dashboard.
- **Dashboard:** Grafana `M3 Production Monitoring` (uid `m3-production`) shows availability, online HitRate@20, license and LLM cost, drift, and collector health.
- **Manual:** anyone who notices a problem (a teammate, the mentor, management) reports it in the same Slack channel. If it meets the bar of an incident, the incident is declared at that point. Issues that don't meet the bar for declaring an incident can be added as a bug ticket in GitHub.
- Known gap: We don't have external health checks to alert on if the whole VM or the monitoring containers die. In this event, alerts will not fire.



## Urgent contact

- **Primary:** post in the team Slack channel `#alerts-and-monitoring` and include `@channel`. Everyone on the team is in the channel, so this pings every member at once.
- **Fallback if nobody answers within 1 hour:** phone, text, and/or email the PM: `Sara Santini`. Contact Information is available in Slack profile.
- **Outside the team:** contact the CMU course staff `Shantanu Shastri` when the VM, network, firewall or Kafka tunnel is the cause of the outage. Any security incident involving a potentially compromised VM must be reported immediately to CMU course staff. Contact Information is available in CMU Online Canvas.
- **If Slack itself is down**, no alert will arrive: use the phone fallback above and check the dashboard and `/recommend/1` by hand.



## Roles


| Role                | Responsibility                                                                                                                                                                               |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Incident Lead       | Owns the response, decides mitigations (along with responding engineer), declares start and end. First person to acknowledge the alert.                                                      |
| Responding Engineer | Runs the mitigation and recovery. This should be the engineer that created or currently maintains the affected part of the system or otherwise is best positioned to execute the mitigation. |


We have no on-call. Everyone on the team is pinged for alerts.

## Communication

- One Slack thread under the alert message is the incident channel. Everything is said there; side conversations are summarized back into it.
- Incident Lead posts a status line every hour until recovery.
- The team must record and retain when the incident starts, when a mitigation was applied and verified, and when the incident ends.
- Never paste secrets, tokens, `.env` contents or user data into Slack; say where they are ("in the VM `.env`") instead.



## Quick mitigations

Run from `/home/production` on the VM (SSH, then `sudo`). Choose the smallest one that fits.

- **Preserve evidence first (two minutes):** container logs rotate (3 × 10 MB) and a restart or recreate loses context. Before changing anything, run `sudo mkdir -p incidents/<date>` and save `sudo docker compose --project-name recommendationmodel logs --no-color --since 2h` plus `sudo docker compose --project-name recommendationmodel ps` and `curl -fsS http://127.0.0.1:8082/health/routing` into it. Keep these files on the VM, outside Git (they can hold user IDs); paste only cleaned excerpts into the incident canvas.
- **Gateway down or not ready:** restart it with `sudo docker compose --project-name recommendationmodel up -d --no-deps gateway`, then check `curl -fsS http://127.0.0.1:8082/health/ready`.
- **Bad or slow model after an update:** roll back by routing: `sudo bash scripts/switch_model.sh blue` (or `green`, whichever is the previous version; see [M3_DEPLOYMENT.md](M3_DEPLOYMENT.md#regular-updates-and-rollback)). Inspect with `... src/deployment.py status`.
- **LLM spend or provider problem:** clear both `OPENAI_API_KEY` and `OPENAI_TOKEN` in the private `.env` (compose falls back to the second one). Then recreate the model servers one slot at a time, never both together: replace the slot that is not serving, wait until it is ready, switch traffic to it, then replace the other (commands: "replace the old blue container" in [M3_DEPLOYMENT.md](M3_DEPLOYMENT.md)). Cold-start profile calls then fail fast and requests use the existing fallback. Restore the keys afterwards the same way.
- **Management asks to stop recommending some movies or a fee class:** switch to the previous model or stop traffic to the affected slot while a fix is prepared; record the request and time in the thread.
- **Disk or storage unhealthy:** `curl -fsS http://127.0.0.1:8082/health/storage`; follow [M3_STORAGE.md](M3_STORAGE.md) before anything is deleted.
- **Kafka events stop:** check that the SSH tunnel on port 9092 is running (it does not survive a VM reboot) and restart it with the course `connect-kafka` script, then restart the `kafka` service.



## Suspected Security Event

Security Incidents (involving suspected secret leak, unauthorized access, etc) should be treated as SEV1. The secrets are the OpenAI key, the Slack webhook, the Grafana admin password, the Kafka tunnel configuration and the VM accounts.

1. Report the suspected compromise to the alerts channel without posting secrets in slack. Name a responding engineer and preserve evidence as above before changing anything.
2. Contain by rotating all secrets that may have been exposed: the OpenAI key in the provider dashboard; the Slack webhook (regenerate it in the Slack app); the Grafana admin password (`grafana cli admin reset-admin-password`); the Kafka tunnel and configuration (course staff reissue the credentials, then rerun `connect-kafka`); and VM SSH keys or accounts if the host itself is suspected (tell the CMU staff).
3. Put the new values only in the VM `.env` and `~/.config/mlip-kafka.conf`, then recreate the affected services one model slot at a time.
4. Find the scope: the saved logs, Grafana users and login records, the VM shell history, and unexpected spend on the OpenAI usage page.
5. If user IDs or event data may have been exposed, tell the mentor and course staff immediately. Do not delete anything that could be evidence.
6. Finish with the recovery checks below, and record in the postmortem what leaked, how, and for how long.



## Recovery and verification

An incident cannot be marked resolved if any of these checks fail:

1. `curl -fsS http://127.0.0.1:8082/health/ready` and `/health/storage` return 200.
2. A real request works: `curl -fsS http://127.0.0.1:8082/recommend/1` returns movie ids.
3. The Grafana alert shows **Normal**.
4. `sudo docker compose --project-name recommendationmodel exec -T api python src/monitoring.py` shows requests succeeding again, so course-visible availability recovered.



## After the incident

- The Incident Lead copies the timeline (alert time, acknowledgement, actions, recovery time) into the incident's Slack canvas (one canvas per incident, titled `Incident <date>`). Incident artifacts (timelines, postmortems, logs) stay out of the repo: it is public and they can contain user IDs or security details.
- Within 48 hours the team writes a short postmortem in the same canvas: cause, impact window, what detection missed, and the follow-up actions. Every action has an owner and a due date and is reviewed at the next team meeting.
- Record time to acknowledge and time to recover in the timeline, and compare them with the targets above.
- Update this plan if a step was wrong or missing.
- Drills follow the same steps. The drill record (timestamps and screenshots, no user data) is the only incident file meant for Git: it lives in `verification/` because the milestone report must link it (see [M3_OPERATIONS.md](M3_OPERATIONS.md)).

