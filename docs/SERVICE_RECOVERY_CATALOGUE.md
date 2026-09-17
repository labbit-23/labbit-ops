# Labit service and recovery catalogue

Status: current-state recovery reference. Secret **locations and key names** belong here; secret values do not.

Inventory date: 2026-09-18 IST

## Recovery order

1. Networking, DNS/private routing, firewall and TLS.
2. PostgreSQL/Supabase database and Storage objects.
3. Labit Core and Shivam Archive.
4. Jasper and report/delivery APIs.
5. UI, Patient and Main.
6. Singleton report workers.
7. Mirth, MWL, DEXA and analyser adapters.
8. Monitoring, collectors, digests and cleanup.

Do not start both copies of a singleton sender/import worker. Do not promote a database standby until the previous writer is fenced.

## Source-code recovery

The local workspace contains multiple independent repositories; `/home/sdrc/projects/labit` itself is not a usable committed repository. GitHub is one copy, but several worktrees contain uncommitted/untracked work.

Run:

```bash
cd /home/sdrc/projects/labit/labit-ops
./scripts/backup-labit-repositories.sh
```

Output goes to ignored `labit-ops/local-backups/<UTC timestamp>/` and contains:

- one verified Git bundle per repository, including all local refs/history;
- a binary worktree patch where tracked files differ from `HEAD`;
- a filtered tar of safe untracked files;
- a manifest with repository, commit, branch, sanitized origin and dirty count;
- SHA-256 checksums for every recovery artifact.

The script deliberately excludes `.env`, private INI/JSON configs, venvs, `node_modules`, builds, logs, report caches and SQLite state. Back those up using encrypted service-state backups, never ordinary source archives.

Restore a bundle with:

```bash
git clone repo.bundle restored-repo
cd restored-repo
git apply ../repo.worktree.patch       # only when listed in MANIFEST.tsv
tar -xf ../repo.untracked.tar           # only when listed in MANIFEST.tsv
```

## Database backups already running locally

The authoritative backup worker is the separate repository:

- Repository: `/home/sdrc/projects/sdrc-infrastructure`
- Engine: `backup/backup.sh`
- Restore tool: `backup/restore.sh`
- Core config: `backup/config/vps2-labit-core.conf`
- Public config: `backup/config/vps2-public.conf`
- Dump command: `backup/scripts/pg_dump_labit_core.sh`
- Archive destination: `backup/archives/`
- Logs: `backup/logs/cron-vps2-labit-core.log` and `cron-vps2-public.log`

Verified on 2026-09-18:

- 15 full `labit_core` schema archives and 14 full `public` schema archives.
- Total local archive size: about 3.7 GB.
- Latest `labit_core` run completed successfully at 2026-09-18 02:12 IST (archive run began 2026-09-17 20:30 UTC).
- Archives have adjacent SHA-256 files.
- Daily retention is 30 days; Sunday copies are retained for 84 days.
- `COMMAND_OUTPUTS_FATAL=true`, so a failed dump prevents publication as a successful archive.
- Encryption and FTP upload are disabled. These are presently local-machine copies, not off-site backups.
- PostgreSQL WAL archiving on VPS2 is off; these are daily logical schema dumps, not point-in-time recovery.

Coverage limits: the two regular jobs cover `labit_core` and `public` schemas. They do not by themselves prove coverage of Supabase Auth schemas, Storage metadata/objects, database roles, cluster-wide globals, Orthanc, Mirth, DEXA inputs, worker state, TLS keys or service secrets. `shivam_archive` has a separate runbook, not evidence of a current daily job.

Before relying on these backups for decommissioning any machine:

1. Copy encrypted archives to a second failure domain.
2. Restore the newest archives into an isolated PostgreSQL instance and run row/schema/application probes.
3. Add cluster globals and every required non-`labit_core`/`public` schema.
4. Back up Supabase Storage objects separately.
5. Record the actual scheduler; the archive cadence is evident, but no matching current user/system cron entry was found during this inventory.

## VPS1 — `labit.sdrc.in` (`10.0.0.3`)

| Runtime service | Installed path / repository | Config location | Port/path | Deploy/recovery entry point |
|---|---|---|---|---|
| Public nginx edge | `/etc/nginx` | `sites-available/labit-ui`, `labbit`, `lab.sdrc.in`, `app.sdrc.in`; TLS outside Git | 80/443; private proxy 8300 | Validate with `nginx -t`, restore sites/certs/firewall, then reload |
| Labit UI (2 PM2 cluster workers) | `/opt/labit/labit-ui`; `labit-ui` | `.env.local` | `127.0.0.1:3001` | `scripts/deploy_labit_ui_pm2.sh`; probe `/login` |
| Patient app | `/opt/labit/labit-patient`; `labit-patient` | `.env.local` | `127.0.0.1:3100` | `scripts/deploy_labit_patient_pm2.sh`; probe `/login` |
| Labit Main / frontend | `/opt/labbit-frontend`; `labit-main` | local env files | `127.0.0.1:3000` | `scripts/deploy-vps-frontend.sh` |
| Labit Core rollback | `/opt/labit/labit-core`; `labit-core` | `.env` | `127.0.0.1:8001` | `scripts/deploy_labit_core_pm2.sh`; probe `/health` |
| Jasper | `/opt/labit/labit-jasper`; `labit-jasper` | PM2 command/token and templates | `*:8089` currently | `scripts/deploy_labit_jasper_pm2.sh`; probe `/health` plus render probes |
| Delivery API | `/opt/labbit-py`; `labit-py` | `.env`, `config.ini`, `services.ini` | `127.0.0.1:8000` | `scripts/deploy-vps-api.sh`; probe `/health` |
| Monitoring agent | `/opt/labbit-py`; `labit-py` | `services.ini`, env | outbound | `scripts/deploy-monitoring-node.sh` |
| Report sender | `/opt/py_utils`; `py_utils` | `workers/report_sender/config/report_sender.json` | worker | `scripts/deploy-vps-report-sender.sh --restart-pm2` |
| Enqueue watcher | `/opt/py_utils`; `py_utils` | same JSON | worker | same deploy script; singleton |
| CTO collector/digest/cleanup | `/opt/labbit-ops`; `labit-ops` | `cto-collector/.env` plus cleanup env | outbound/scheduled | `scripts/deploy-vps-ops.sh` |
| Shivam Archive | `/opt/shivam-archive`; separate `shivam-archive` repo | `.env` | `0.0.0.0:8010` | repository deploy/runbook; probe health and archive lookup |
| Rehearsal stack | `/opt/*-rehearsal` | local env files | 8093/8094 and internal | restore only when rehearsal is required |

VPS1 deployed commit inventory on 2026-09-18 is retained in the repository backup manifest and should be refreshed automatically in a future inventory command. Important drift observed: deployed UI commit `bc7a0b204e07` differs from local `34a307380ab7`; determine whether this is expected before recovery testing.

## VPS2 — `supabase.sdrc.in` (`10.0.0.2`)

| Runtime service | Installed path / repository | Config/state location | Port/path | Deploy/recovery entry point |
|---|---|---|---|---|
| Labit Core primary app | `/opt/labit/labit-core`; `labit-core` | `.env` | `10.0.0.2:8001` | `scripts/deploy_labit_core_pm2.sh`; `/health` |
| PostgreSQL | Supabase Docker Compose, `supabase-db` | `/opt/supabase/docker`; Docker volume | private 5432/5433/6543 | Restore database first; `pg_isready`; schema/application probes |
| Supabase Auth | `supabase-auth` container | Compose env + DB schemas | via Kong/private | container health plus login probe |
| PostgREST | `supabase-rest` | Compose env + DB | via Kong | REST authenticated read probe |
| Storage | `supabase-storage` | DB metadata plus object volume/backend | via Kong | object upload/read probe and independent object restore |
| Studio/Meta/Kong/Pooler | Supabase Compose | `/opt/supabase/docker` | local/public nginx routes | Docker health and controlled API probes |
| CTO collector | `/opt/labbit-ops`; `labit-ops` | `cto-collector/.env` | outbound | `scripts/deploy-vps-ops.sh` |
| nginx | `/etc/nginx/sites-available/supabase.sdrc.in` | TLS outside Git | 80/443 | `nginx -t`, reload, external health |

Core is deployed at commit `4234ba591acb`. Its origin currently points to a deleted temporary bundle, so recovery must use the repository bundle/GitHub or replace the origin before the next normal deploy.

## Site and device services

| Service | Expected host | Repository/config | Recovery note |
|---|---|---|---|
| Mirth Connect | `sdrc-h81` | integrations repo; exported channel XML plus configuration map | Export channels and code templates; record Java/Mirth versions and plugins; keep credentials encrypted |
| Radiology MWL workers | `sdrc-h81` | `py_utils/workers/radiology_mwl`; private `config/mwl_worker_*.json`; systemd units | Preserve SQLite state/outbox; real modality worklist query is the final probe |
| Orthanc | separate Windows host over Tailscale | Orthanc config, plugins, worklist directory and image store | Back up state separately; validate DICOM ingest/query, not only HTTP |
| DEXA collector | GE Lunar Windows workstation | `labit-dexa/labit-dexa`; private `.env` | Preserve service/task definition, watched paths and raw inputs; test upload |
| DEXA reporting app | SDRC Ubuntu host | `labit-dexa/sdrc-dexa-app`; `.env.local` | Build/start under PM2; test patient list, image and PDF |
| Other HL7/ASTM/device adapters | lab integration hosts | `/projects/integrations` and per-adapter configs | Catalogue each Windows service/systemd/task, COM/TCP endpoint and durable queue |

These hosts still require a live inventory. They must not be inferred from repository READMEs alone.

## Configuration and dependency rules

- Document key **names**, owner and rotation procedure; store values only in encrypted secret backup.
- Every service record must name its upstream and downstream: UI/Patient/Main -> Core; Core -> PostgreSQL, Jasper and Archive; workers -> Core/Supabase/WhatsApp; machine adapters -> Core; DEXA -> Supabase DB/Storage.
- Config files are host-specific. Do not copy the entire VPS1 `.env` to another host without reviewing bind addresses, URLs and credentials.
- Record firewall source rules and private DNS/IPs alongside nginx/systemd/PM2 configuration.
- PM2's saved process list is not sufficient recovery documentation; the repository deploy script and health probe are authoritative.
- Generated artifacts can be rebuilt only if runtime/toolchain versions and lock files are retained. Jasper's tested JAR and templates should also be captured in a release capsule.

## Remaining work to call this disaster-recovery complete

- Inventory `sdrc-h81`, Orthanc, DEXA and every integration host live.
- Inventory env key names (never values), nginx route maps, UFW rules, crons/timers and systemd units on every host.
- Add encrypted off-site copies for database archives, repository recovery sets and service state.
- Add Supabase cluster-wide and Storage-object backup coverage.
- Perform and time an isolated full restore.
- Automate catalogue generation so commit/service/config drift is visible.
