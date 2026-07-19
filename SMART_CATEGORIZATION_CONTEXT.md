# Smart Categorization Engine — Copilot & Frontend Context

> **Backend**: CimplrCorpSaaS-Go  
> **Branch**: nyneos  
> **All endpoints**: `POST`, `Content-Type: application/json`  
> **Base URL**: `{GATEWAY_URL}` (e.g. `http://localhost:8082` for cash service)

---

## Table of Contents

1. [What Was Built](#1-what-was-built)
2. [DB Schema Changes](#2-db-schema-changes)
3. [Updated Existing Endpoint](#3-updated-existing-endpoint)
4. [New Endpoints — Full API Reference](#4-new-endpoints)
5. [Confidence Score Bands](#5-confidence-score-bands)
6. [Classification Steps Explained](#6-classification-steps-explained)
7. [Typical Frontend Flows](#7-typical-frontend-flows)
8. [File Map](#8-file-map)

---

## 1. What Was Built

A **6-step smart categorization waterfall** that runs on every bank statement transaction. The engine:

1. Matches explicit **category rules** (exact/contains/regex narration + amount + channel)
2. Looks up the **counterparty** name (from `mastercounterparty`) and uses its default category
3. Looks up the transaction's **GL account** → mapped category
4. Checks **analyst corrections** — if this exact (or very similar) narration was corrected before, reuse that category
5. Runs **fuzzy similarity** (PostgreSQL pg_trgm) against previously corrected narrations (> 70% match)
6. Marks as **UNALLOCATED** if nothing matched

Every classification writes an **immutable audit log** entry and, when confidence is below 0.90, queues the transaction for **analyst review**.

---

## 2. DB Schema Changes

Migration file: `migration_smart_categorization.sql` (already applied to Supabase)

### Columns added to `cimplrcorpsaas.bank_statement_transactions`

| Column | Type | Description |
|---|---|---|
| `narration_clean` | `TEXT` | Raw description with bank prefix/codes stripped |
| `narration_stemmed` | `TEXT` | Porter-stemmed + financial abbreviation expanded form (used for fuzzy matching) |
| `narration_ref` | `TEXT` | Extracted reference number (UTR, cheque no., UPI ref) |
| `payment_channel` | `TEXT` | NEFT / RTGS / IMPS / UPI / NACH / ACH / CHEQUE / INTERNAL |
| `confidence_score` | `NUMERIC(5,4)` | 0.0000–1.0000; NULL means not yet classified by smart engine |
| `classification_step` | `TEXT` | Which step in the waterfall matched (RULE/COUNTERPARTY/GL/CORRECTION/SIMILARITY/UNALLOCATED) |

### New Tables

#### `cimplrcorpsaas.categorization_corrections`
Stores every human correction so the engine learns from it (Step 4).
```sql
correction_id   BIGSERIAL PK
narration_clean TEXT NOT NULL
narration_stemmed TEXT NOT NULL  -- GIN indexed for trgm
category_id     TEXT NOT NULL
corrected_by    TEXT NOT NULL
corrected_at    TIMESTAMPTZ DEFAULT now()
transaction_id  BIGINT
entity_id       TEXT
is_active       BOOLEAN DEFAULT TRUE
```

#### `cimplrcorpsaas.classification_audit_log`
**Immutable** — never UPDATE or DELETE rows here. Every classification (system or human) is appended.
```sql
log_id              BIGSERIAL PK
transaction_id      BIGINT NOT NULL
category_id         TEXT NOT NULL
confidence          NUMERIC(5,4)
classification_step TEXT
rule_id             BIGINT            -- which rule_id matched (nullable)
source_ref          TEXT              -- e.g. 'manual_correction', 'rule:42'
classified_by       TEXT              -- 'system' or analyst name
classified_at       TIMESTAMPTZ DEFAULT now()
```

#### `cimplrcorpsaas.categorization_review_queue`
Holds transactions that need analyst review (confidence < 0.90 or UNALLOCATED).
```sql
queue_id        BIGSERIAL PK
transaction_id  BIGINT UNIQUE NOT NULL
suggested_cat   TEXT                   -- category_id suggested by engine
confidence      NUMERIC(5,4)
step            TEXT                   -- which step suggested it
ai_reasoning    TEXT
status          TEXT DEFAULT 'PENDING' -- PENDING | CONFIRMED | CORRECTED | DISMISSED
reviewed_by     TEXT
reviewed_at     TIMESTAMPTZ
created_at      TIMESTAMPTZ DEFAULT now()
```

#### `cimplrcorpsaas.gl_category_mapping`
Maps a GL account ID to a category (feeds Step 3 of the waterfall).
```sql
gl_account_id   TEXT PK
category_id     TEXT NOT NULL
created_by      TEXT
created_at      TIMESTAMPTZ DEFAULT now()
```

---

## 3. Updated Existing Endpoint

### `POST /cash/bank-statements/v2/transactions`

**No request changes.** Each item in `data[]` now includes 5 new fields:

```jsonc
{
  "transaction_id": 9001,
  "entity_name": "Cimplr India Pvt Ltd",
  "tran_id": "TXN123",
  "value_date": "2026-04-05T00:00:00Z",
  "transaction_date": "2026-04-05T00:00:00Z",
  "description": "NEFT/ICICI/PAY/SAL/APR2026/UTR123456",
  "withdrawal_amount": 500000,
  "deposit_amount": 0,
  "balance": 1200000,
  "category_name": "Salaries",
  "category_id": "CAT_001",          // ← was missing before
  "misclassified_flag": false,

  // ── NEW smart categorization fields ──────────────────────
  "narration_clean": "salary april 2026",
  "narration_ref": "UTR123456",
  "payment_channel": "NEFT",
  "confidence_score": 0.92,          // null if not yet classified
  "classification_step": "RULE"      // "" if not yet classified
}
```

---

## 4. New Endpoints

### 4.1 `POST /cash/smart-cat/status`
Live dashboard snapshot of the categorization engine.

**Request**
```json
{
  "user_id": "user@company.com",
  "entity_id": "ENT_001"   // optional — omit for all entities
}
```

**Response**
```json
{
  "success": true,
  "totals": {
    "transactions": 10000,
    "categorized": 9200,
    "uncategorized": 800,
    "categorized_pct": 92.0
  },
  "by_step": {
    "RULE": 7500,
    "COUNTERPARTY": 300,
    "GL": 100,
    "CORRECTION": 150,
    "SIMILARITY": 650,
    "UNALLOCATED": 300
  },
  "confidence": {
    "high_pct": 80.0,
    "medium_pct": 12.0,
    "low_pct": 8.0
  },
  "review_queue": {
    "PENDING": 350,
    "CONFIRMED": 150,
    "CORRECTED": 100,
    "DISMISSED": 50
  },
  "correction_memory_size": 250,
  "last_run": "2026-04-21T18:00:00Z",
  "last_run_ago_mins": 120
}
```

---

### 4.2 `POST /cash/smart-cat/review-queue`
Paginated list of transactions awaiting analyst review.

**Request**
```json
{
  "user_id": "user@company.com",
  "entity_id": "ENT_001",    // optional filter
  "status": "PENDING",       // PENDING | CONFIRMED | CORRECTED | DISMISSED (default: PENDING)
  "limit": 50,               // max 500, default 100
  "offset": 0
}
```

**Response**
```json
{
  "success": true,
  "total": 350,
  "limit": 50,
  "offset": 0,
  "rows": [
    {
      "queue_id": 1,
      "transaction_id": 9001,
      "suggested_cat": "CAT_001",
      "suggested_cat_name": "Salaries",
      "confidence": 0.76,
      "step": "SIMILARITY",
      "status": "PENDING",
      "created_at": "2026-04-21T18:00:00Z",
      "account_number": "001234567890",
      "account_nickname": "ICICI Main",
      "description": "NEFT/ICICI/PAY/SAL/APR2026",
      "narration_clean": "salary april 2026",
      "narration_ref": "UTR123456",
      "payment_channel": "NEFT",
      "withdrawal": 500000,
      "deposit": null,
      "value_date": "2026-04-05",
      "entity_id": "ENT_001"
    }
  ]
}
```

---

### 4.3 `POST /cash/smart-cat/review-action`
Analyst confirms, corrects, or dismisses a queued transaction.

**Request — CONFIRM** (accept the engine's suggestion as-is)
```json
{
  "user_id": "analyst@company.com",
  "transaction_id": 9001,
  "action": "CONFIRM"
}
```

**Request — CORRECT** (analyst picks a different category)
```json
{
  "user_id": "analyst@company.com",
  "transaction_id": 9001,
  "action": "CORRECT",
  "category_id": "CAT_007"
}
```

**Request — DISMISS** (skip for now, don't categorize)
```json
{
  "user_id": "analyst@company.com",
  "transaction_id": 9001,
  "action": "DISMISS"
}
```

**Response** (all three actions)
```json
{
  "success": true,
  "transaction_id": 9001,
  "action": "CORRECT"
}
```

> **Important**: `CORRECT` saves the correction into `categorization_corrections` so the engine learns from it for all future runs. This is the primary way the model improves over time.

---

### 4.4 `POST /cash/smart-cat/correction`
Standalone analyst correction — can be called directly from the transaction list without going through the review queue.

**Request**
```json
{
  "user_id": "analyst@company.com",
  "transaction_id": 9001,
  "category_id": "CAT_007"
}
```

**Response**
```json
{
  "success": true,
  "transaction_id": 9001,
  "category_id": "CAT_007"
}
```

---

### 4.5 `POST /cash/smart-cat/gl-mapping/create`
Map a GL account ID to a category. This feeds Step 3 (GL lookup) of the waterfall for all future transactions that come in on that GL account.

**Request**
```json
{
  "user_id": "admin@company.com",
  "gl_account_id": "GL_1234",
  "category_id": "CAT_007"
}
```

**Response**
```json
{ "success": true }
```

---

## 5. Confidence Score Bands

Use `confidence_score` from `/cash/bank-statements/v2/transactions` to show badges:

| Score | Band | Suggested UI |
|---|---|---|
| `>= 0.90` | **High** | Green badge — "Auto-categorized" |
| `0.70 – 0.89` | **Medium** | Amber badge — "Review suggested" |
| `< 0.70` | **Low** | Red badge — "Needs review" |
| `null` | **Unprocessed** | Grey badge — "Not classified" |

---

## 6. Classification Steps Explained

| `classification_step` | What it means | Typical confidence |
|---|---|---|
| `RULE` | Matched an explicit category rule (narration/amount/channel) | 0.95–1.00 |
| `COUNTERPARTY` | Matched a known counterparty name → default category | 0.90 |
| `GL` | GL account mapped to a category via gl_category_mapping table | 0.88 |
| `CORRECTION` | Exact or near-exact match to a previous analyst correction | 0.85–0.99 |
| `SIMILARITY` | Trigram fuzzy match (> 70%) to a corrected narration | 0.65–0.90 |
| `UNALLOCATED` | No match in any step — needs human categorization | 0.00 |
| `""` (empty) | Transaction not yet processed by the smart engine | — |

---

## 7. Typical Frontend Flows

### A. Transaction List Page (existing)
- Call `POST /cash/bank-statements/v2/transactions` as before
- Show new `payment_channel` chip (NEFT / UPI / etc.)
- Show `confidence_score` badge (use bands from §5)
- Show `narration_clean` as secondary text under the raw description
- Add "Correct category" button → calls `POST /cash/smart-cat/correction`

### B. Smart Categorization Dashboard (new page)
1. Call `POST /cash/smart-cat/status` → render KPI cards:
   - `totals.categorized_pct` — doughnut chart
   - `by_step` — bar chart (how transactions were classified)
   - `confidence` — stacked bar (High / Medium / Low %)
   - `review_queue.PENDING` — "X transactions need review" alert
   - `last_run` / `last_run_ago_mins` — "Last run: 2h ago"

### C. Review Queue Page (new page)
1. Call `POST /cash/smart-cat/review-queue` with `status: "PENDING"`
2. For each row show: narration_clean, suggested_cat_name, confidence badge, payment_channel
3. Analyst actions:
   - **Confirm** → `POST /cash/smart-cat/review-action` `{ action: "CONFIRM" }`
   - **Correct** → open category picker → `POST /cash/smart-cat/review-action` `{ action: "CORRECT", category_id }`
   - **Dismiss** → `POST /cash/smart-cat/review-action` `{ action: "DISMISS" }`
4. After action: remove row from list, refresh count badge

### D. GL Mapping Settings (admin page)
- Form: GL Account ID + Category dropdown
- On submit: `POST /cash/smart-cat/gl-mapping/create`

---

## 8. File Map

| File | Purpose |
|---|---|
| `migration_smart_categorization.sql` | All DDL — run once on DB |
| `internal/services/categorizer/narration.go` | Narration pre-processing: strip bank prefixes, detect channel, extract ref, stem |
| `internal/services/categorizer/types.go` | Shared types: `TxnInput`, `ClassificationResult`, step constants |
| `internal/services/categorizer/core.go` | 6-step waterfall engine + pgx persist logic |
| `api/cash/bankstatement/reviewQueue.go` | HTTP handlers: review-queue, review-action, correction, gl-mapping |
| `api/cash/bankstatement/smartCatStatus.go` | HTTP handler: status dashboard |
| `internal/jobs/cash/categorizationProcessor.go` | Cron job: nightly full recategorization (pgx, no sql.DB) |
| `api/cash/cash.go` | Route registration (5 new `/cash/smart-cat/*` routes) |
| `api/cash/bankstatement/handlers.go` | `GetBankStatementTransactionsHandler` updated with 5 new fields |

---

## Key Rules for Future Changes

1. **Never use `database/sql` or `lib/pq`** in any new smart-cat code. Use `pgxpool.Pool` exclusively.
2. **Never UPDATE or DELETE** rows in `classification_audit_log`. It is append-only.
3. **`PersistBatch`** in `core.go` uses `pgx.Batch` — keep it that way for performance. It does 4 ops per transaction (narration update, category update, audit log insert, review queue upsert).
4. **Circular import rule**: `api/cash/bankstatement` imports `internal/jobs/cash`, so `internal/jobs/cash` must NOT import `api/cash/bankstatement`. Shared logic goes in `internal/services/categorizer/`.
5. **Confidence threshold** `MinConfidenceForActuals = 0.70` is defined in `internal/services/categorizer/types.go`. Change it there to affect the entire engine.
6. **Adding new waterfall steps**: add a new `StepXxx` constant in `types.go`, add the lookup function in `core.go`, call it inside `SmartCategorize` between the existing steps.
