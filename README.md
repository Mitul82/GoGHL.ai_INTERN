# Switchboard

Switchboard is a multi-tenant, real-time shared team inbox for customer support. Customers write in via a simulated messaging provider (**Pigeon**), and human support agents sign in through an OpenID Connect (**OIDC + PKCE**) identity provider to triage, claim, close/reopen, and reply to conversations in real time.

## Architecture Overview

- **Backend (`scaffold/server`)**: Express + TypeScript (`tsx` runtime) with a raw HTTP server hosting both REST routes and native WebSockets (`ws`).
  - `src/config.ts`: Environment-driven configuration with defaults matching the grading harness.
  - `src/db.ts`: Singleton MongoDB connection helper (`getDb()`).
  - `src/redis.ts`: Singleton `ioredis` client (`getRedis()`).
  - `src/types.ts`: Shared TypeScript domain interfaces (`SessionPayload`, `ConversationDoc`, `InboundMessageDoc`, `OutboundMessageDoc`, `EventDoc`).
  - `src/store.ts`: MongoDB index setup, Redis Pub/Sub real-time fan-out, monotonic per-workspace sequence numbering (`seq`), WebSocket catch-up replay (`?after=<seq>`), per-workspace rate limiting, and the lease-based outbound delivery worker.
  - `src/index.ts`: OIDC Authorization Code + PKCE (`S256`) login/callback, signed HTTP-only session cookies, workspace-isolated inbox/reply/claim endpoints, Pigeon HMAC-SHA256 webhook verification, WebSocket upgrade handling, and `/__grader/*` endpoints.
- **Frontend (`scaffold/web`)**: Vite + React 19 + TypeScript agent dashboard supporting workspace switching, live conversation lists, message threads with delivery status tracking (`queued`, `sent`, `delivered`, `failed`), atomic conversation claiming, open/closed toggles, and automatic WebSocket reconnection with `?after=<seq>` catch-up.
- **Infrastructure**: MongoDB (durable storage and lease locking), Redis (atomic sequence counters, Pub/Sub cross-instance event fan-out, PKCE verifiers, and per-workspace rate-limit/cool-off state).

## Prerequisites

- Node.js `>= 22`
- Docker & Docker Compose (or local MongoDB `:27017` and Redis `:6379` with `npm run mocks`)

## Running from a Clean Clone

1. **Start infrastructure and simulators** (MongoDB, Redis, Pigeon `:4010`, Mock IdP `:4020`, Paybox `:4030`):
   ```bash
   npm run up

```

*(Without Docker: run MongoDB and Redis locally, then run `npm run mocks`.)*

2. **Install dependencies**:
```bash
npm run scaffold:install

```


3. **Start the backend server** (listens on `PORT`, default `4000`):
```bash
npm run start:server

```


*(For hot-reload development: `npm run scaffold:server`.)*
4. **Start the React web app** (in a separate terminal, runs on `http://localhost:5173`):
```bash
npm run scaffold:web

```


5. **Sign in**: Open `http://localhost:5173`, click **Sign in with IdP**, and choose one of the test users (e.g., `u_ben` for `ws_acme`, `u_chloe` for `ws_globex`, or `u_eli` for both workspaces).

## Running Checks

Stop any manually running server on port `4000` before running the harness (the harness launches instances automatically via `switchboard.json` behind a load balancer on `:4000`):

```bash
npm run check               # Level 1 check
npm run check -- --levels 5 # Level 5 check (OIDC, isolation, WebSockets)
npm run check:full          # Levels 1, 2, 3, and 5; writes CHECK_REPORT.json

```

## Environment Variables

All configuration is read from environment variables (or a root `.env` file if present) in `scaffold/server/src/config.ts`:

| Variable | Default | Description |
| --- | --- | --- |
| `PORT` | `4000` | HTTP/WebSocket server listen port |
| `WEB_ORIGIN` | `http://localhost:5173` | Frontend URL redirected to after OIDC callback |
| `GRADER_TOKEN` | `grader-dev-token` | Shared secret for `/__grader/*` endpoints |
| `MONGO_URL` | `mongodb://localhost:27017/switchboard` | MongoDB connection URI |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URI |
| `PIGEON_URL` | `http://localhost:4010` | Pigeon provider base URL |
| `PIGEON_WEBHOOK_SECRET` | `whsec_pigeon_dev` | HMAC-SHA256 secret for `x-pigeon-signature` |
| `PIGEON_KEY_WS_ACME` | `pk_acme_7d1f0a` | Pigeon API key for `ws_acme` |
| `PIGEON_KEY_WS_GLOBEX` | `pk_globex_c93b22` | Pigeon API key for `ws_globex` |
| `OIDC_ISSUER` | `http://localhost:4020` | Mock IdP issuer URL |
| `OIDC_CLIENT_ID` | `switchboard-web` | OIDC client ID |
| `OIDC_CLIENT_SECRET` | `dev-secret-not-for-production` | OIDC client secret |
| `OIDC_REDIRECT_URI` | `http://localhost:4000/auth/callback` | OIDC callback URL |