# Infrastructure notes — 2026-09-18

## Decisions and direction

- Rebuilding the former Oracle server at Ctrl-S as an Ubuntu primary is only an option, not an active migration plan.
- Current VPS1/VPS2 performance and uptime may make the Ctrl-S server unnecessary. Reassess near month-end; relinquish it if production remains stable and recovery safeguards are adequate.
- Do not move services merely to consolidate them. Mirth, MWL, DEXA collectors, Orthanc and analyser adapters may need to remain close to their devices/LAN.
- The immediate useful work is documentation, reproducible packaging, inventory and recovery—not another server move.

## Current production arrangement

- VPS1, `labit.sdrc.in`, remains the public edge and runs UI, Patient, Main, Python delivery services, report workers, Jasper, Shivam Archive, monitoring and a rollback Core.
- VPS2, `supabase.sdrc.in`, runs Supabase/PostgreSQL and the Core currently serving UI and machine API traffic.
- VPS1 and VPS2 Core copies must both remain until Patient/Python/local dependencies are explicitly migrated and tested.
- The current VPS2 Core checkout has a stale temporary-bundle Git origin. The running service is unaffected, but recovery should use GitHub or the new repository recovery bundles.

## Database-backup finding

- The infrastructure backup worker is in `/home/sdrc/projects/sdrc-infrastructure/backup` on this local development machine.
- Archives land in `/home/sdrc/projects/sdrc-infrastructure/backup/archives`.
- Verified on 2026-09-18: 15 `labit_core` archives, 14 `public` archives, approximately 3.7 GB total, with SHA-256 sidecars.
- The latest Core backup completed successfully on 2026-09-18 IST.
- Retention: daily copies for 30 days; Sunday copies for 84 days.
- Active backup configs have encryption disabled and FTP upload disabled. These copies currently share the local machine's failure domain.
- The regular jobs are logical dumps of `labit_core` and `public`, not PostgreSQL point-in-time recovery. VPS2 WAL archiving is off.
- Coverage is not yet proven for Supabase Auth and other schemas, roles/globals, Storage objects, Orthanc, Mirth, DEXA inputs, worker state, TLS material or secrets.

## Source-code backup finding

- The Labit workspace is a collection of 14 independent Git repositories; the workspace root is not a usable committed repository.
- Several repositories had local uncommitted/untracked work, so remote Git repositories alone were not a complete backup.
- A verified 207 MB repository recovery set was created at:
  `/home/sdrc/projects/labit/labit-ops/local-backups/20260917T220026Z`
- It contains all-ref Git bundles, worktree patches where needed, filtered safe untracked files, a manifest and SHA-256 checksums.
- Secrets, private configs, dependency caches, logs and generated state were intentionally excluded.
- Repeat with `labit-ops/scripts/backup-labit-repositories.sh`.

## Documentation created

- `labit-ops/docs/UNIFIED_DEPLOYMENT_ARCHITECTURE.md` — optional Ctrl-S topology, placement matrix, release capsules, backup/failover design and migration gates.
- `labit-ops/docs/SERVICE_RECOVERY_CATALOGUE.md` — service-to-host/repository/config/deploy/health mapping and current backup coverage.
- `labit-ops/scripts/backup-labit-repositories.sh` — repeatable source recovery-set generator.

## Before giving up Ctrl-S

1. Observe acceptable VPS1/VPS2 uptime, peak latency, capacity and machine-ingest reliability through month-end.
2. Make an encrypted copy of DB and repository backups in a second failure domain.
3. Perform an isolated database restore and record the result and duration.
4. Add required Supabase schemas/globals and Storage-object coverage.
5. Preserve a verified final image/archive of legacy Oracle data if any retention requirement remains.
6. Confirm that no active service still requires `120.138.8.37`.
7. Document live Mirth, Orthanc, DEXA and analyser-host installations and their private configuration/state.

## Next practical task

Complete the live inventory of site/device hosts and produce an encrypted off-site copy of both the database archives and repository recovery set. Do not change production routing while doing this.
