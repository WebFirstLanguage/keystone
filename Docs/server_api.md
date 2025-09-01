# Keystone Server API (JWT)

This document describes the HTTP API exposed by the `keystone-server` binary and how JWT authentication works.

## Overview

- Auth: JWT (HS256) via `Authorization: Bearer <token>` header.
- All endpoints except `/health` and `/auth/login` require a valid JWT.
- Backed by the in-repo `keystone` datastore (redb adapter).

## Configuration

- `BIND_ADDR` (default: `127.0.0.1:8080`)
- `DATASTORE_PATH` (default: `./data/keystone.redb`)
- `JWT_SECRET` (required in production; if unset, an ephemeral secret is generated)
- `JWT_ISSUER` (default: `keystone`)
- `JWT_AUDIENCE` (optional)
- `AUTH_USERNAME` (default: `admin`)
- `AUTH_PASSWORD` (default: `admin`)

## Authentication

### Login

- `POST /auth/login`
  - Body: `{ "username": string, "password": string }`
  - 200 OK: `{ "token": string, "token_type": "Bearer", "expires_in": number }`
  - 401 Unauthorized on invalid credentials.

### Using the Token

Include in all protected requests:

```
Authorization: Bearer <token>
```

## Health

- `GET /health` → 200 `{ "status": "ok" }`

## Buckets

- `POST /buckets`
  - Body: `{ "name": string }`
  - 201 Created
  - 400 Invalid name; 409 if exists.

- `DELETE /buckets/{name}`
  - 204 No Content
  - 404 if not found

## Objects

- `GET /buckets/{name}/objects?prefix=...`
  - 200 `{ "keys": [string, ...] }`

- `PUT /buckets/{name}/objects/{key}`
  - Body: raw bytes
  - 201 Created

- `GET /buckets/{name}/objects/{key}`
  - 200 application/octet-stream bytes
  - 404 if missing

- `DELETE /buckets/{name}/objects/{key}`
  - 204 No Content
  - 404 if missing

## Notes

- Object payloads are stored as raw bytes (internally serialized via bincode as `Vec<u8>`).
- Adjustments can be made to align with additional requirements (claims, roles, expiry) if needed.

