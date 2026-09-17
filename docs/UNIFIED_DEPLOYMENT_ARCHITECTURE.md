# Unified Labit deployment architecture

Status: proposed next infrastructure task; no migration is authorized by this document.

Last updated: 2026-09-18 (Asia/Kolkata)

## Decision in one page

Rebuild the retiring Oracle server from scratch as a supported Linux host and make it the **Indian primary** only after it passes hardware, storage, network, restore, and failover gates. Keep the current VPS estate during the transition. Use VPS2 as the first off-site database/standby location and VPS1 as the public edge plus warm application fallback. If budget permits, retain both VPS nodes: they remove different failure modes and are more useful than treating two copies on the Indian server as redundancy.

The target is not “put everything on one box.” It is:

1. One fast domestic primary for the latency-sensitive application and database path.
2. One or two geographically separate VPS nodes capable of serving a controlled failover.
3. Site-bound integration workers close to their machines.
4. Immutable, checksummed release capsules so every host runs a known release.
5. Backups that are separate from replicas and are restore-tested.

Do **not** move Mirth or machine-protocol bridges away from the laboratory merely for consolidation. They depend on LAN reachability, modality protocols, local queues, and in some cases Tailscale paths. Centralise their release and monitoring, not necessarily their execution.

## Facts currently verified

### VPS1 — `labit.sdrc.in`

- Hostname: `ubuntu-4gb-hel1-1`; public `204.168.203.180`; private `10.0.0.3`.
- 2 vCPU, about 4 GB RAM, 4 GB swap, 80 GB root disk.
- Currently runs the public nginx edge, `labit-ui`, `labit-patient`, `labit-main`, `labbit-api`, report workers, Jasper, Shivam Archive, monitoring, and a rollback copy of Core.
- UI traffic and `/machine-api/*` currently reach the VPS2 Core over the private network.
- Patient and some Python/report paths still depend on the VPS1 Core or other VPS1-local services. The old Core must therefore remain available until those dependencies are explicitly migrated and tested.

### VPS2 — `supabase.sdrc.in`

- Hostname: `ubuntu-4gb-hel1-2`; public `77.42.43.194`; private `10.0.0.2`.
- 4 vCPU, about 8 GB RAM, 4 GB swap, 161 GB root disk.
- Runs Supabase/PostgreSQL containers, Core on private port 8001, nginx, and an ops collector.
- Core and Supabase are healthy at the time of inventory. Core dependencies back to VPS1 are source-filtered by UFW.
- The deployed Core checkout has a stale temporary-bundle Git origin. Runtime is unaffected, but this is not an acceptable release or recovery mechanism.

### Site-bound and special-purpose nodes

- `sdrc-h81` runs Mirth and already has proven paths to Orthanc and modalities. Radiology MWL workers are intended to run there.
- Orthanc is on a separate Windows system reachable through Tailscale.
- The DEXA flow includes a Windows collector at the GE Lunar workstation and an Ubuntu-hosted web/reporting app.
- Machine integrations also include protocol converters and analyser-specific adapters under the integrations estate.
- `120.138.8.37` is the old Oracle/Shivam server, not VPS1. Existing templates still mention Oracle/Tomcat ports, although important report-status paths have already been decoupled through Labit Core/Shivam Archive.

### New information to verify before provisioning

The retiring Oracle server may be wiped and rebuilt. Its “approximately 12 GB” RAM, CPU model, disk media/health, RAID state, controller cache, network uplink, public/private addresses, Ctrl-S support/SLA, power arrangement, and remote-console access remain unverified. “Ctrl-S server” and “domestic/on-prem server” must also be reconciled: the same design works in either case, but the failure and access assumptions differ.

## Recommended target topology

```text
Users / partners / machines
            |
      DNS and HTTPS edge
       /              \
VPS1 edge/warm app   optional VPS edge
       \              /
        Indian primary (fresh Linux)
          | app services + primary DB/storage
          |
     encrypted replication
          |
     VPS2 standby DB + warm Core

Lab LAN: Mirth, MWL and analyser adapters -> authenticated HTTPS edge/Core
Independent monitors on VPS1/VPS2 and one site node observe every tier.
Immutable backups go to a third failure domain, not only either replica.
```

Use a supported Ubuntu LTS release selected at installation time, minimal server profile, encrypted administrative access, unattended security updates with a maintenance policy, host firewall, time synchronisation, SMART/RAID monitoring, and no desktop packages. Keep the old Oracle data disks or a verified image read-only until archive completeness and legal retention are signed off.

### Capacity verdict

Twelve GB can run the present stack, but it is a **minimum**, not comfortable redundancy. Current live evidence shows roughly 2.2 GB in use on VPS1 and 1.8 GB on VPS2, while caches and container accounting make simple addition imperfect. Next builds, two UI workers, Jasper, PostgreSQL, Supabase services, report rendering, and burst traffic need headroom.

- 12 GB: acceptable for a measured first primary, with swap, strict service limits, and alerts; do not promise every auxiliary service on it.
- 16 GB: preferred minimum for combined app + database primary.
- 32 GB: comfortable target if the hardware supports an inexpensive upgrade.
- SSD/NVMe health and latency matter more than “tons of disk.” A large aging HDD array could make this slower than VPS2.

Before moving PostgreSQL, run a 24–72 hour burn-in and collect CPU, memory, disk latency/IOPS, database benchmark, packet loss, and application load-test results. A memory upgrade is strongly preferred if practical.

## Service placement matrix

| Service or responsibility | Current primary | Target primary | Warm/standby | Reason / constraint |
|---|---|---|---|---|
| Public DNS/TLS/nginx | VPS1 | VPS1 initially; optionally managed edge later | VPS2 or second edge | Keeps public ingress independent of the Indian primary and gives a fast rollback point. |
| `labit-ui` | VPS1 | Indian primary after burn-in | VPS1, deployable on VPS2 | Stateless build; run two local workers only after memory measurement. |
| `labit-patient` | VPS1 | Indian primary | VPS1 | Remove the deploy script's localhost-only Core assumption first. |
| `labit-main` | VPS1 | Indian primary | VPS1 | Inventory its direct DB and Core assumptions before moving. |
| `labit-core` | VPS2, with VPS1 rollback copy | Indian primary | VPS2 warm Core; VPS1 temporary rollback | Latency-sensitive API and machine-ingest path. Exactly one write-primary DB endpoint must be active. |
| PostgreSQL/Supabase | VPS2 | Indian primary only after restore and replication drills | VPS2 asynchronous standby | Stateful and highest-risk move. Preserve VPS2 as source until the new primary proves stable. |
| Supabase API/Auth/Storage | VPS2 | Indian primary if required by actual consumers | VPS2 | Move as a tested stack, not container by container. Verify object storage replication separately from PostgreSQL. |
| Jasper renderer | VPS1 | Indian primary | VPS1 | CPU/memory bursts; private-only authenticated endpoint; keep compiled artifact in release capsule. |
| `labbit-api` / delivery API | VPS1 | Indian primary | VPS1 | Must first eliminate remaining localhost and legacy Oracle fallbacks. |
| Report sender / enqueue watcher | VPS1 | Indian primary | Installed but stopped on VPS1 | **Singleton workers**: use a DB lease/advisory lock; never run active-active without idempotency proof. |
| Shivam Archive | VPS1 | Indian primary or VPS2 | VPS1 | Read-only legacy history; preserve independent archive backup. No live Oracle write dependency. |
| DEXA web/report app | separate Ubuntu host | Indian primary if network tests pass | present Ubuntu host/VPS | Stateless app can move; workstation collector remains close to GE Lunar. |
| DEXA Windows collector | GE Lunar workstation | stay local | spare imaged workstation/manual procedure | Device/filesystem-bound. Centralise configuration backup and health reporting. |
| Mirth Connect | `sdrc-h81` | stay on `sdrc-h81` initially | documented rebuild package on a second site-capable node | LAN/device-bound and already has proven Orthanc paths. Do not trade reliability for cosmetic consolidation. |
| MWL, ASTM/HL7 and analyser adapters | lab/integration machines | stay beside devices | per-adapter cold/warm package | Need LAN access and durable local outboxes for internet outages. |
| Orthanc | separate Windows host | stay until separately redesigned | verified backup/restore or standby | Stateful imaging store; requires its own capacity and recovery design. |
| Monitoring collector | VPS1/VPS2 and local roles | VPS1 + VPS2 + site node | each other | A host cannot be the only monitor of itself. |
| Logs | per host | local journal/files plus remote central sink | second retained copy | Avoid losing incident evidence with the failed host. Redact PHI and secrets. |
| Backups | not yet evidenced as restore-tested | primary -> immutable third location + VPS copy | periodic offline/export copy | A replica is not a backup. |

## The simpler-than-Docker release collection

Call the unit a **Labit release capsule**. It is a versioned directory plus a manifest, not a fleet of hand-edited Git checkouts. Supabase may remain Docker Compose because its upstream stack is container-native; the Labit applications do not need to be containerised merely for consistency.

```text
/opt/labit/
  releases/
    2026.09.18-1/
      RELEASE.json
      SHA256SUMS
      core/                 # source + locked wheelhouse or built venv input
      ui/                   # Next standalone build + static assets
      patient/              # Next standalone build
      main/                 # built application
      jasper/labit-jasper.jar
      workers/              # pinned worker code and lock files
      migrations/           # ordered, checksummed, forward migration set
      units/                # versioned systemd unit templates
      probes/               # health and smoke tests
  current -> releases/2026.09.18-1
  previous -> releases/2026.09.17-3
  shared/
    cache/
    uploads/
    state/
/etc/labit/                 # host-specific env/secrets, mode 0600
```

`RELEASE.json` records the release ID, every repository commit, build timestamps, supported schema range, runtime versions, artifact checksums, services included, and minimum host resources. Build once in CI or a controlled builder, verify signatures/checksums on the target, switch `current` atomically, restart/reload in dependency order, run probes, and point back to `previous` if application checks fail.

Important limits:

- Database migrations are generally forward-only. Application rollback must declare the schema versions it supports; a symlink alone cannot undo data changes.
- Secrets, mutable queues, SQLite worker state, generated reports, uploaded objects, logs, and database files never belong inside a capsule.
- Python must use hashed lock files and a wheelhouse; Node must use `npm ci` and preferably Next standalone output; Java ships the already-tested JAR and template checksums.
- Git pull on production becomes an emergency-only workflow. The broken temporary-bundle origin on VPS2 disappears as part of adopting capsules.
- Use systemd for the clean host. PM2 can remain during transition, but mixing PM2 and systemd ownership for the same process is prohibited.

The first small deliverable should be a `labit-release` command with `build`, `verify`, `install`, `activate`, `probe`, `rollback`, and `inventory` operations. It should orchestrate existing service-specific checks rather than replace them.

## Data, replication, and failover

### PostgreSQL

Start with asynchronous PostgreSQL streaming replication from the Indian primary to VPS2, encrypted over a private tunnel/Tailscale/WireGuard. Archive WAL continuously to immutable object storage for point-in-time recovery. Choose exact retention only after measuring change volume.

Initial failover must be **manual and fenced**:

1. Establish that the old primary is stopped or isolated from writers.
2. Record the last received/replayed WAL position and expected data-loss window.
3. Promote VPS2.
4. change the single database service endpoint or application secret.
5. run write/read and clinical workflow probes.
6. do not reintroduce the old primary until it has been rebuilt as a replica.

Automatic failover before reliable fencing risks split brain and is worse than a short controlled outage. Add an orchestrator only after repeated drills.

### Supabase storage and auxiliary state

PostgreSQL replication does not copy every filesystem object. Inventory Supabase Storage's actual backend and replicate/backup its objects independently. Do the same for Orthanc images, generated PDFs that cannot be regenerated, Mirth configuration/channels, local integration outboxes, DEXA raw inputs, worker SQLite state, and encryption/signing keys.

### Worker safety

Report sending, reconciliation, imports, cleanup, and digests must be labelled either singleton, idempotent active-active, or scheduled one-shot. Singleton workers acquire a database-backed lease with expiry and identify their node in every job. Failover is not “start all PM2 processes on both VPSes.”

### Backups

Use a 3-2-1-style policy:

- at least three copies of important data;
- at least two failure domains/media types;
- at least one encrypted, immutable/offline copy separate from the primary provider and credentials.

Back up PostgreSQL with base backups plus WAL/PITR, object storage, configuration (without putting plaintext secrets in ordinary archives), release manifests, Mirth channels, Jasper templates, Orthanc configuration/data, and host recovery notes. Run automated verification daily and a real isolated restore drill monthly initially. Monitoring must report the last **successful restore verification**, not merely “backup job exited zero.”

Provisional recovery objectives, subject to owner approval:

| Capability | Target RPO | Target RTO |
|---|---:|---:|
| Core clinical DB | 5 minutes | 30–60 minutes |
| Machine-result ingest | local durable queue; at most 5 minutes after replay | 60 minutes |
| UI/API application tier | no data loss | 15–30 minutes |
| Reports/images/object storage | 15 minutes | 2 hours |
| Analytics/MIS/monitoring history | 24 hours | 1 business day |

## Monitoring and security baseline

- Monitor externally from VPS1/VPS2 and internally from the lab: HTTPS workflows, Core health, DB replication lag, WAL archive age, queue depth, last machine message by channel, Mirth channel state, report dispatch age, disk latency/space, memory/swap, certificates, backup verification, and release drift.
- Send alerts through a path that does not depend solely on the failed Labit stack.
- Bind databases, Core backends, Jasper, and admin ports to private interfaces only. Public nginx is the controlled ingress.
- Use per-service credentials with least privilege. Rotate the machine API and database credentials during/after migration; never copy one global `.env` to every node.
- Keep host-specific secrets under `/etc/labit`, root/service-readable only; store an encrypted recovery copy under separate credentials.
- Preserve audit logs and clock synchronisation. Central logs must avoid patient names/report bodies unless explicitly required and access-controlled.
- Maintain a host/service inventory generated from the release manifest and actual process/listener probes; alert on drift.

## Migration phases and gates

### Phase 0 — discovery and acceptance

1. Confirm whether the candidate is in Ctrl-S, physically on-site, or both descriptions refer to different hosts.
2. Record CPU, firmware, RAM/ECC capability, disks/RAID/SMART, NICs, public/private networking, console access, warranty/SLA, power and cooling.
3. Take and verify a final Oracle image/archive before wiping anything.
4. Catalogue every active service, port, cron, PM2/systemd process, env-key name, state directory, external dependency, and data owner across all nodes.
5. Classify every worker for singleton/idempotency and every datastore for backup/replication.

Gate: signed inventory; no unknown production listener or scheduled task; owner confirms Oracle retention.

### Phase 1 — clean Linux and burn-in

Install minimal supported Ubuntu LTS, patch it, harden SSH/firewall, set private networking, configure time, monitoring and remote console. Stress CPU/RAM and storage for 24–72 hours. Measure disk latency and packet loss to the lab, VPS1 and VPS2.

Gate: zero hardware errors; stable network; tested console recovery; acceptable p95 storage/database latency; resource upgrade decision made.

### Phase 2 — release capsule and stateless canary

Build the capsule tool and package Core, UI, patient, main, Jasper, Python API/workers, ops collector, and DEXA web app. Deploy stateless services to the new host without changing public traffic. Use a private canary hostname and synthetic/approved test records.

Gate: artifact checksums match; all health probes pass; rollback to `previous` is timed and proven; no plaintext secret in a capsule.

### Phase 3 — application cutover with DB still on VPS2

Route a controlled portion or explicit test path to Indian app services while PostgreSQL remains on VPS2. This separates application-host risk from database-migration risk. Measure full workflows and WAN database latency.

Gate: registration, billing, collection, analyser/Mirth ingest, approval, PDF, patient access, WhatsApp/report dispatch, DEXA and MIS tests pass; error rate and latency are within agreed thresholds.

### Phase 4 — database rehearsal and primary move

Create a fresh replica on the Indian host, rehearse promotion using a scrubbed/restored copy, measure catch-up and rollback, then schedule a brief write freeze for final promotion. Retain VPS2 as the standby and backup source only after validation.

Gate: two successful restore tests, two successful promotion/failback drills, measured RPO/RTO, object storage accounted for, explicit go/no-go owner approval.

### Phase 5 — simplify and drill

Remove obsolete localhost assumptions, dead Oracle fallbacks and duplicate process managers only after observability proves no use. Keep VPS1 and VPS2 roles documented. Run quarterly failure drills and monthly restore tests; review capacity trends.

Gate: no undocumented production dependency; runbooks usable by someone other than the implementer.

## Go/no-go checklist for making the rebuilt server primary

All must be true:

- Hardware identity and 24–72 hour burn-in passed.
- SSD/RAID health and write latency passed; RAM headroom survives peak render/build/load tests.
- Ctrl-S/site power, network, console and support failure modes are documented.
- Final Oracle archive is verified and retained separately.
- Capsule install and one-command application rollback have been demonstrated.
- Database restore, replication, promotion and fencing have been demonstrated.
- Supabase object storage and all non-Postgres state are backed up.
- Singleton workers cannot double-send or double-import during failover.
- Mirth/machine/DEXA paths pass real channel tests, not only HTTP health checks.
- Independent monitoring and alert delivery remain available when the primary is powered off.
- Named operator, maintenance window, rollback trigger and communication plan exist.

## Immediate next actions (read-only first)

1. Obtain the new server's exact hardware/controller/network inventory and clarify its physical/hosting location.
2. Generate a machine-readable service inventory from VPS1, VPS2, `sdrc-h81`, the DEXA host/workstation, Orthanc and the old Oracle server image.
3. Map env **key names** and dependencies without copying secret values into documentation.
4. Inventory Supabase storage volumes and current backup/WAL configuration.
5. Draft `RELEASE.json` schema and package one low-risk service as the capsule proof of concept.
6. Agree RPO/RTO and whether 12 GB will be upgraded before any database move.

Until those six are complete, the safe position is: keep today's VPS1/VPS2 production arrangement, keep both Core instances, and make no DNS/database-primary change.
