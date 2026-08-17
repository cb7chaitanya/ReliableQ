-- Initial schema: jobs, job_attempts, charges.
--
-- `updated_at` is maintained by application code on every guarded UPDATE,
-- not by a trigger, so every writer is forced to reason about it explicitly
-- (see DESIGN.md sec. 4: explicit SQL over hidden behavior).

create extension if not exists pgcrypto;

create table jobs (
    id                  uuid primary key default gen_random_uuid(),
    kind                text not null,
    payload             jsonb not null,
    status              text not null check (status in ('PENDING', 'RUNNING', 'SUCCEEDED', 'DEAD')),
    attempts            integer not null default 0,
    max_attempts        integer not null,
    next_attempt_at     timestamptz not null,
    lease_token         uuid null,
    lease_expires_at    timestamptz null,
    last_error_code     text null,
    last_error_message  text null,
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now(),
    started_at          timestamptz null,
    finished_at         timestamptz null,
    version             bigint not null default 0,

    constraint jobs_attempts_non_negative check (attempts >= 0),
    constraint jobs_max_attempts_positive check (max_attempts >= 1),
    constraint jobs_attempts_within_budget check (attempts <= max_attempts),
    constraint jobs_lease_both_or_neither check ((lease_token is null) = (lease_expires_at is null)),
    constraint jobs_pending_has_no_lease_or_finish
        check (status <> 'PENDING' or (lease_token is null and finished_at is null)),
    constraint jobs_running_has_lease
        check (status <> 'RUNNING' or (lease_token is not null and lease_expires_at is not null)),
    constraint jobs_terminal_has_finished_and_no_lease
        check (status not in ('SUCCEEDED', 'DEAD') or (finished_at is not null and lease_token is null))
);

-- Supports claiming due pending jobs (worker sec. 9.1).
create index idx_jobs_pending_due on jobs (next_attempt_at, created_at) where status = 'PENDING';

-- Supports reclaiming expired leases (worker sec. 9.1, sec. 9.3).
create index idx_jobs_running_lease_expiry on jobs (lease_expires_at) where status = 'RUNNING';

-- Supports GET /v1/jobs?status=...&cursor=... listing.
create index idx_jobs_status_created_at on jobs (status, created_at, id);

create table job_attempts (
    id                  uuid primary key default gen_random_uuid(),
    job_id              uuid not null references jobs (id),
    attempt_number      integer not null,
    worker_id           text not null,
    lease_token         uuid null,
    started_at          timestamptz not null default now(),
    finished_at         timestamptz null,
    outcome             text null check (outcome is null or outcome in ('SUCCEEDED', 'RETRY_SCHEDULED', 'DEAD', 'LEASE_LOST')),
    error_code          text null,
    error_message       text null,
    scheduled_delay_ms  bigint null,
    duration_ms         bigint null,

    constraint job_attempts_unique_attempt unique (job_id, attempt_number)
);

create index idx_job_attempts_job_id on job_attempts (job_id, attempt_number);

create table charges (
    id                  uuid primary key default gen_random_uuid(),
    idempotency_key     text not null unique,
    customer_id         text not null,
    amount_cents        bigint not null check (amount_cents > 0),
    currency            text not null,
    created_at          timestamptz not null default now()
);
