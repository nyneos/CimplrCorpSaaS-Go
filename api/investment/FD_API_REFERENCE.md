# Fixed Deposit (FD) Module — End-to-End API Reference

> **Base URL** `https://<host>`  
> **Auth** All routes require a valid session. Pass `user_id` in the JSON body (resolved against the in-memory session store). Requests without a matching session return `401 Unauthorized`.  
> **Content-Type** `application/json` for all write operations.  
> **Approval Engine** Write operations fire `CreateInstance` / `RecordAction` asynchronously (goroutine) after `tx.Commit`. The HTTP response is never blocked by approval-engine work.  
> **Notifications** All state-transition handlers fire `catalog.TriggerNotification` asynchronously after commit.

---

## Sample Reference IDs

| Concept | Sample Value |
|---|---|
| `entity_id` | `ENT-0000001` |
| `user_id` | `USR-001` |
| `bank_id` | `BANK-001` |
| `bank_account_id` | `BACC-001` |
| `booking_id` | `FDBK-20240101-001` |
| `confirmation_id` | `FDCF-20240101-001` |
| `fd_id` | `FDMST-20240101-001` |
| `run_id` | `FDACR-RUN-001` |
| `receipt_id` | `FDREC-001` |
| `exception_id` | `FDEX-001` |
| `reconcile_run_id` | `RECON-RUN-001` |

---

## Happy-Path Flow

```
1. POST /investment/fd/booking/create           → booking_id (status: DRAFT → APPROVAL_PENDING)
2. POST /investment/fd/booking/approve          → booking approved (status: APPROVED)
3. POST /investment/fd/booking/send-to-bank     → booking marked SENT_TO_BANK
4. POST /investment/fd/confirmation/capture     → confirmation_id (status: PENDING_CONFIRMATION or VARIANCE_DETECTED)
5. POST /investment/fd/confirmation/resolve-variance  [if variance]
6. POST /investment/fd/confirmation/approve     → confirmation approved (status: CONFIRMED)
7. POST /investment/fd/master/activate          → fd_id (status: APPROVAL_PENDING)
8. POST /investment/fd/master/approve           → FD activated (status: ACTIVE)
9. POST /investment/fd/accrual/run/create       → run_id (status: DRAFT)
10. POST /investment/fd/accrual/run/validate    → scope validated
11. POST /investment/fd/accrual/run/execute     → accrual calculated
12. POST /investment/fd/accrual/run/submit      → run submitted (status: PENDING_APPROVAL)
13. POST /investment/fd/accrual/run/approve     → run approved, journals auto-posted
14. POST /investment/fd/receipt/create          → receipt_id (status: CAPTURED)
15. POST /investment/fd/receipt/submit          → receipt submitted (status: APPROVAL_PENDING)
16. POST /investment/fd/receipt/bulk-approve    → receipt approved (status: APPROVED)
17. POST /investment/fd/receipt/post-journals   → journal entries created
18. POST /investment/fd/receipt/reconcile/run   → reconcile_run_id (async)
```

---

## Module 1 — FD Booking Workbench

### POST `/investment/fd/booking/create`
Creates a single FD booking request.

**Request**
```json
{
  "user_id":            "USR-001",
  "entity_id":          "ENT-0000001",
  "bank_id":            "BANK-001",
  "bank_account_id":    "BACC-001",
  "principal_amount":   10000000,
  "interest_rate":      7.25,
  "tenure_days":        365,
  "value_date":         "2024-01-01",
  "maturity_date":      "2025-01-01",
  "interest_payout_frequency": "QUARTERLY",
  "auto_renewal":       false,
  "remarks":            "Q1 treasury FD"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `user_id` | string | ✓ | Must match active session |
| `entity_id` | string | ✓ | |
| `bank_id` | string | ✓ | |
| `bank_account_id` | string | ✗ | FK to bank account |
| `principal_amount` | float64 | ✓ | > 0 |
| `interest_rate` | float64 | ✓ | Annual %, e.g. 7.25 |
| `tenure_days` | int | ✓ | |
| `value_date` | string | ✓ | `YYYY-MM-DD` |
| `maturity_date` | string | ✓ | `YYYY-MM-DD` |
| `interest_payout_frequency` | string | ✗ | `MONTHLY`, `QUARTERLY`, `AT_MATURITY` |
| `auto_renewal` | bool | ✗ | default false |
| `remarks` | string | ✗ | |

**Response `200`**
```json
{
  "success": true,
  "data": {
    "booking_id": "FDBK-20240101-001",
    "entity_id":  "ENT-0000001",
    "requested":  "treasurer@corp.com"
  }
}
```

**Side effects**
- Inserts into `investment.fd_booking_request` (status `DRAFT`)
- Inserts into `investment.fd_audit_booking_request` (action `CREATE`, status `PENDING_APPROVAL`)
- Async: `CreateInstance(FD_BOOKING)` → flips status to `APPROVAL_PENDING` if matrix configured
- Async: `TriggerNotification` event `FD_BOOKING_SUBMITTED`

---

### POST `/investment/fd/booking/create-bulk`
Creates multiple FD bookings in a single call. Each row is processed in its own transaction; partial success is supported.

**Request**
```json
{
  "user_id": "USR-001",
  "rows": [
    {
      "entity_id":       "ENT-0000001",
      "bank_id":         "BANK-001",
      "principal_amount": 5000000,
      "interest_rate":   7.0,
      "tenure_days":     180,
      "value_date":      "2024-01-01",
      "maturity_date":   "2024-07-01"
    },
    {
      "entity_id":       "ENT-0000002",
      "bank_id":         "BANK-002",
      "principal_amount": 2000000,
      "interest_rate":   7.5,
      "tenure_days":     365,
      "value_date":      "2024-01-01",
      "maturity_date":   "2025-01-01"
    }
  ]
}
```

**Response `200`**
```json
{
  "success": true,
  "data": [
    { "row_index": 0, "booking_id": "FDBK-20240101-002", "entity_id": "ENT-0000001", "success": true },
    { "row_index": 1, "booking_id": "FDBK-20240101-003", "entity_id": "ENT-0000002", "success": true }
  ]
}
```

Each row result has `success: false` and `error` string on failure.

---

### POST `/investment/fd/booking/update`
Edits a booking in `DRAFT`, `REJECTED`, or `APPROVAL_PENDING` status. Cancels any in-flight approval instance and creates a new `FD_BOOKING_EDIT` instance.

**Request**
```json
{
  "user_id":    "USR-001",
  "booking_id": "FDBK-20240101-001",
  "reason":     "Corrected rate",
  "fields": {
    "interest_rate":  7.50,
    "principal_amount": 10500000
  }
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `user_id` | string | ✓ | |
| `booking_id` | string | ✓ | |
| `reason` | string | ✓ | Stored in audit trail |
| `fields` | object | ✓ | Key-value pairs of columns to update |

**Response `200`**
```json
{ "success": true, "data": { "booking_id": "FDBK-20240101-001", "requested": "treasurer@corp.com" } }
```

---

### POST `/investment/fd/booking/delete`
Soft-deletes one or more bookings (creates a `FD_BOOKING_DELETE` approval instance). Cannot delete `APPROVED` or `SENT_TO_BANK` bookings.

**Request**
```json
{
  "user_id":    "USR-001",
  "booking_ids": ["FDBK-20240101-001"],
  "reason":     "Duplicate entry"
}
```

**Response `200`**
```json
{
  "success": true,
  "data": [
    { "booking_id": "FDBK-20240101-001", "success": true }
  ]
}
```

---

### POST `/investment/fd/booking/approve`
Bulk-approves one or more bookings. Engine-first: calls `RecordAction` if an approval instance is pending for the user; falls back to direct DB stamp when no instance exists.

**Request**
```json
{
  "user_id":     "USR-002",
  "booking_ids": ["FDBK-20240101-001", "FDBK-20240101-002"],
  "comment":     "Approved as per policy"
}
```

**Response `200`**
```json
{
  "success": true,
  "data": {
    "engine_acted": 2,
    "direct_acted": 0,
    "errors": [],
    "checker": "approver@corp.com"
  }
}
```

---

### POST `/investment/fd/booking/reject`
Bulk-rejects bookings. Same engine-first logic as approve.

**Request**
```json
{
  "user_id":     "USR-002",
  "booking_ids": ["FDBK-20240101-001"],
  "comment":     "Rate not competitive"
}
```

**Response `200`**
```json
{
  "success": true,
  "data": { "engine_acted": 1, "direct_acted": 0, "errors": [], "checker": "approver@corp.com" }
}
```

---

### GET `/investment/fd/booking/all`
Returns all bookings with latest audit row, approval workflow state, and bank account details.

**Query params** `entity_id=ENT-0000001` (optional)

**Response** Array of booking objects with nested `audit` and `approval_workflow` keys.

---

### GET `/investment/fd/booking/detail`
Returns one booking with full audit history and inline approval workflow.

**Query params** `booking_id=FDBK-20240101-001&user_id=USR-001`

---

### GET `/investment/fd/booking/audit`
Returns audit history for a specific booking.

**Query params** `booking_id=FDBK-20240101-001`

---

### GET `/investment/fd/booking/approved-active`
Returns bookings in `APPROVED` or `SENT_TO_BANK` status (eligible for confirmation capture).

**Query params** `entity_id=ENT-0000001`

---

### POST `/investment/fd/booking/send-to-bank`
Marks approved bookings as `SENT_TO_BANK`.

**Request**
```json
{
  "user_id":     "USR-001",
  "booking_ids": ["FDBK-20240101-001"],
  "sent_date":   "2024-01-02"
}
```

**Response `200`**
```json
{ "success": true, "data": { "updated_count": 1, "marked_by": "treasurer@corp.com" } }
```

---

## Module 2 — FD Confirmation Workbench

### POST `/investment/fd/confirmation/capture`
Captures bank-confirmed FD details against an approved booking. Detects principal/rate/maturity-date variance automatically.

**Request**
```json
{
  "user_id":                      "USR-001",
  "booking_id":                   "FDBK-20240101-001",
  "confirmed_principal_amount":   10000000,
  "confirmed_interest_rate":      7.25,
  "confirmed_maturity_date":      "2025-01-01",
  "confirmed_value_date":         "2024-01-01",
  "fd_reference_number":          "SBI/FD/2024/0001",
  "bank_advice_date":             "2024-01-02",
  "remarks":                      "As per bank advice letter"
}
```

| Field | Type | Required |
|---|---|---|
| `user_id` | string | ✓ |
| `booking_id` | string | ✓ |
| `confirmed_principal_amount` | float64 | ✓ |
| `confirmed_interest_rate` | float64 | ✓ |
| `confirmed_maturity_date` | string | ✓ |
| `confirmed_value_date` | string | ✓ |
| `fd_reference_number` | string | ✗ |
| `bank_advice_date` | string | ✗ |
| `remarks` | string | ✗ |

**Response `200`**
```json
{
  "success": true,
  "data": {
    "confirmation_id":             "FDCF-20240101-001",
    "booking_id":                  "FDBK-20240101-001",
    "has_variance":                false,
    "threshold_breached":          false,
    "confirmation_status":         "PENDING_CONFIRMATION",
    "rate_variance":               0,
    "amount_variance":             0,
    "maturity_date_variance_days": 0,
    "requested":                   "treasurer@corp.com"
  }
}
```

If `has_variance: true`, `confirmation_status` becomes `VARIANCE_DETECTED`. If `threshold_breached: true`, a `FD_CONFIRMATION_VARIANCE_RESOLVE` approval instance is fired.

---

### POST `/investment/fd/confirmation/resolve-variance`
Resolves a detected variance (accept or adjust).

**Request**
```json
{
  "user_id":          "USR-001",
  "confirmation_id":  "FDCF-20240101-001",
  "variance_action":  "ACCEPT",
  "comment":          "Variance within acceptable range"
}
```

`variance_action` values: `ACCEPT`, `ADJUST`, `REJECT`

**Response `200`**
```json
{
  "success": true,
  "data": {
    "confirmation_id":    "FDCF-20240101-001",
    "variance_action":    "ACCEPT",
    "new_status":         "PENDING_CONFIRMATION",
    "booking_status":     "SENT_TO_BANK",
    "threshold_breached": false,
    "resolved_by":        "treasurer@corp.com"
  }
}
```

---

### POST `/investment/fd/confirmation/approve`
Bulk-approves confirmations. Engine-first with direct-stamp fallback.

**Request**
```json
{
  "user_id":          "USR-002",
  "confirmation_ids": ["FDCF-20240101-001"],
  "comment":          "Confirmed with bank statement"
}
```

**Response `200`**
```json
{ "success": true, "data": { "engine_acted": 1, "direct_acted": 0, "errors": [], "checker": "approver@corp.com" } }
```

---

### POST `/investment/fd/confirmation/reject`
Bulk-rejects confirmations.

**Request** Same shape as approve (use `confirmation_ids`).

---

### POST `/investment/fd/confirmation/delete`
Soft-deletes confirmations not yet CONFIRMED.

**Request**
```json
{
  "user_id":          "USR-001",
  "confirmation_ids": ["FDCF-20240101-001"],
  "reason":           "Wrong booking linked"
}
```

**Response `200`**
```json
{
  "success": true,
  "data": [{ "confirmation_id": "FDCF-20240101-001", "success": true }]
}
```

---

### GET `/investment/fd/confirmation/detail`
Returns one confirmation with audit history and approval workflow.

**Query params** `confirmation_id=FDCF-20240101-001&user_id=USR-001`

---

### GET `/investment/fd/confirmation/all`
Returns all confirmations with latest audit row.

**Query params** `entity_id=ENT-0000001` (optional)

---

### GET `/investment/fd/confirmation/audit`
Returns audit history for a confirmation.

**Query params** `confirmation_id=FDCF-20240101-001`

---

### GET `/investment/fd/confirmation/confirmed`
Returns confirmations in `CONFIRMED` status (eligible for FD master activation).

**Query params** `entity_id=ENT-0000001`

---

## Module 3 — FD Master (Activation)

### POST `/investment/fd/master/activate`
Creates an FD master record from a confirmed confirmation. Generates cashflow schedule automatically.

**Request**
```json
{
  "user_id":         "USR-001",
  "confirmation_id": "FDCF-20240101-001"
}
```

**Response `200`**
```json
{
  "success": true,
  "data": {
    "fd_id":           "FDMST-20240101-001",
    "confirmation_id": "FDCF-20240101-001",
    "cashflow_count":  4,
    "requested_by":    "treasurer@corp.com"
  }
}
```

`cashflow_count` = number of cashflow schedule rows generated (based on `interest_payout_frequency` + tenure).

**Side effects**
- Inserts into `investment.fd_master` (status `APPROVAL_PENDING`)
- Inserts cashflow rows into `investment.fd_cashflow_schedule`
- Async: `CreateInstance(FD_MASTER_CREATE)` → flips to `APPROVAL_PENDING` if matrix configured
- Async: `TriggerNotification` event `FD_ACTIVATION_SUBMITTED`

---

### POST `/investment/fd/master/approve`
Bulk-approves FD master activations. Engine-first with direct-stamp fallback.

**Request**
```json
{
  "user_id": "USR-002",
  "fd_ids":  ["FDMST-20240101-001"],
  "comment": "Approved"
}
```

**Response `200`**
```json
{ "success": true, "data": { "engine_acted": 1, "direct_acted": 0, "errors": [], "checker": "approver@corp.com" } }
```

---

### POST `/investment/fd/master/reject`
Bulk-rejects FD master activations.

**Request** Same shape as approve (use `fd_ids`).

---

### GET `/investment/fd/master/detail`
Returns one FD master record with audit history and approval workflow.

**Query params** `fd_id=FDMST-20240101-001&user_id=USR-001`

---

### GET `/investment/fd/master/all`
Returns all FD master records with latest audit row and approval workflow state.

**Query params** `entity_id=ENT-0000001` (optional)

---

### GET `/investment/fd/master/audit`
Returns audit history for an FD master record.

**Query params** `fd_id=FDMST-20240101-001`

---

### GET `/investment/fd/master/cashflows`
Returns the cashflow schedule (interest + principal repayment dates) for an FD.

**Query params** `fd_id=FDMST-20240101-001`

**Response `200`** Array of cashflow rows:
```json
{
  "success": true,
  "data": [
    { "cashflow_date": "2024-04-01", "cashflow_type": "INTEREST", "amount": 181250 },
    { "cashflow_date": "2024-07-01", "cashflow_type": "INTEREST", "amount": 181250 },
    { "cashflow_date": "2024-10-01", "cashflow_type": "INTEREST", "amount": 181250 },
    { "cashflow_date": "2025-01-01", "cashflow_type": "INTEREST", "amount": 181250 },
    { "cashflow_date": "2025-01-01", "cashflow_type": "PRINCIPAL", "amount": 10000000 }
  ]
}
```

---

### GET `/investment/fd/master/journals`
Returns journal entries linked to an FD.

**Query params** `fd_id=FDMST-20240101-001`

---

## Module 4 — FD Accrual

### POST `/investment/fd/accrual/run/create`
Creates a new accrual run (draft) for a given entity and accrual period.

**Request**
```json
{
  "user_id":      "USR-001",
  "entity_id":    "ENT-0000001",
  "period_start": "2024-01-01",
  "period_end":   "2024-03-31",
  "mode":         "DAILY"
}
```

`mode` values: `DAILY`, `MONTHLY`, `PERIOD`

**Response `200`**
```json
{
  "success": true,
  "data": {
    "run_id":       "FDACR-RUN-001",
    "entity_id":    "ENT-0000001",
    "period_start": "2024-01-01",
    "period_end":   "2024-03-31",
    "run_status":   "DRAFT"
  }
}
```

---

### POST `/investment/fd/accrual/run/validate`
Validates the scope of an accrual run — identifies eligible FDs and blockers.

**Request**
```json
{ "user_id": "USR-001", "run_id": "FDACR-RUN-001" }
```

**Response `200`**
```json
{
  "success": true,
  "data": {
    "run_id":           "FDACR-RUN-001",
    "eligible_count":   12,
    "blocker_count":    0,
    "run_status":       "VALIDATED"
  }
}
```

---

### POST `/investment/fd/accrual/run/execute`
Executes accrual calculations for all eligible FDs in the run.

**Request**
```json
{ "user_id": "USR-001", "run_id": "FDACR-RUN-001" }
```

**Response `200`**
```json
{
  "success": true,
  "data": { "run_id": "FDACR-RUN-001", "calculated": 12, "failed": 0 }
}
```

---

### POST `/investment/fd/accrual/run/submit`
Submits an executed accrual run for approval.

**Request**
```json
{ "user_id": "USR-001", "run_id": "FDACR-RUN-001" }
```

**Response `200`**
```json
{ "success": true, "data": { "run_id": "FDACR-RUN-001", "status": "PENDING_APPROVAL" } }
```

**Side effects**
- Updates `fd_accrual_run.run_status = 'PENDING_APPROVAL'`
- Async: `CreateInstance(FD_ACCRUAL_APPROVE)`
- Async: `TriggerNotification` event `FD_ACCRUAL_RUN_SUBMITTED`

---

### POST `/investment/fd/accrual/run/approve`
Bulk-approves accrual runs. On final approval, auto-posts accrual journal entries.

**Request**
```json
{
  "user_id":  "USR-002",
  "run_ids":  ["FDACR-RUN-001"],
  "role_id":  "ROLE-TREASURY-MGR",
  "comment":  "Approved — period Q1 2024"
}
```

**Response `200`** Array per run_id:
```json
{
  "success": true,
  "data": [
    { "run_id": "FDACR-RUN-001", "success": true, "status": "POSTED", "period_locked": "Q1-2024" }
  ]
}
```

`status` is `POSTED` when this was the final approval eye (journals auto-created), `PENDING_APPROVAL` if more eyes remain.

---

### POST `/investment/fd/accrual/run/reject`
Bulk-rejects accrual runs.

**Request**
```json
{
  "user_id": "USR-002",
  "run_ids": ["FDACR-RUN-001"],
  "comment": "Recalculate — rate basis incorrect"
}
```

**Response `200`** Array per run_id with `status: "REJECTED"`.

---

### GET `/investment/fd/accrual/run/all`
Returns all accrual runs for an entity.

**Query params** `entity_id=ENT-0000001`

---

### GET `/investment/fd/accrual/ledger`
Returns ledger rows (per-FD interest amounts) for a run.

**Query params** `run_id=FDACR-RUN-001`

---

### GET `/investment/fd/accrual/detail`
Returns the detail view of a single accrual run.

**Query params** `run_id=FDACR-RUN-001&user_id=USR-001`

---

### GET `/investment/fd/accrual/findings`
Returns validation findings (eligible FDs, blockers, warnings) for a run.

**Query params** `run_id=FDACR-RUN-001`

---

### GET `/investment/fd/accrual/execution-log`
Returns the step-by-step execution log for a run.

**Query params** `run_id=FDACR-RUN-001`

---

### POST `/investment/fd/accrual/override/propose`
Proposes an accrual amount override for a specific FD within a run.

**Request**
```json
{
  "user_id":       "USR-001",
  "run_id":        "FDACR-RUN-001",
  "fd_id":         "FDMST-20240101-001",
  "override_amount": 185000,
  "reason":        "Manual calculation per bank advice"
}
```

**Response `200`**
```json
{ "success": true, "data": { "run_id": "FDACR-RUN-001", "fd_id": "FDMST-20240101-001", "override_amount": 185000 } }
```

---

### POST `/investment/fd/accrual/override/approve`
Approves a proposed override.

**Request**
```json
{ "user_id": "USR-002", "run_id": "FDACR-RUN-001", "fd_id": "FDMST-20240101-001", "comment": "Verified" }
```

---

### POST `/investment/fd/accrual/override/reject`
Rejects a proposed override.

**Request** Same shape as approve.

---

### POST `/investment/fd/accrual/schedule/create`
Creates an automated accrual schedule (cron-based).

**Request**
```json
{
  "user_id":    "USR-001",
  "entity_id":  "ENT-0000001",
  "cron_expr":  "0 2 * * *",
  "mode":       "DAILY"
}
```

---

### POST `/investment/fd/accrual/schedule/update`
Updates an existing schedule.

### POST `/investment/fd/accrual/schedule/disable`
Disables a schedule.

### POST `/investment/fd/accrual/schedule/enable`
Re-enables a disabled schedule.

### POST `/investment/fd/accrual/schedule/approve`
Approves a new/updated schedule.

### POST `/investment/fd/accrual/schedule/reject`
Rejects a new/updated schedule.

### POST `/investment/fd/accrual/schedule/delete`
Soft-deletes a schedule.

### GET `/investment/fd/accrual/schedule/all`
Returns all schedules.

**Query params** `entity_id=ENT-0000001`

---

## Module 5 — FD Receipt

### POST `/investment/fd/receipt/create`
Captures an interest/maturity receipt against an active FD.

**Request**
```json
{
  "user_id":                "USR-001",
  "fd_id":                  "FDMST-20240101-001",
  "entity_id":              "ENT-0000001",
  "receipt_date":           "2024-04-01",
  "gross_interest_received": 181250,
  "tds_amount_deducted":    18125,
  "bank_account_id":        "BACC-001",
  "receipt_type":           "INTEREST",
  "remarks":                "Q1 interest receipt"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `user_id` | string | ✓ | |
| `fd_id` | string | ✓ | |
| `entity_id` | string | ✓ | |
| `receipt_date` | string | ✓ | `YYYY-MM-DD` |
| `gross_interest_received` | float64 | ✓ | |
| `tds_amount_deducted` | float64 | ✗ | default 0 |
| `bank_account_id` | string | ✗ | |
| `receipt_type` | string | ✗ | `INTEREST`, `MATURITY`, `PREMATURE` |
| `remarks` | string | ✗ | |

**Response `200`**
```json
{
  "success":                true,
  "receipt_id":             "FDREC-001",
  "fd_id":                  "FDMST-20240101-001",
  "fd_ref_no":              "SBI/FD/2024/0001",
  "gross_interest_received": 181250,
  "tds_amount_deducted":    18125,
  "net_amount_received":    163125,
  "receipt_status":         "CAPTURED"
}
```

---

### POST `/investment/fd/receipt/update`
Edits a receipt in `CAPTURED` or `REJECTED` status. Cancels any in-flight approval instance.

**Request**
```json
{
  "user_id":    "USR-001",
  "receipt_id": "FDREC-001",
  "reason":     "Corrected TDS",
  "fields": {
    "tds_amount_deducted": 18200,
    "gross_interest_received": 181250
  }
}
```

**Response `200`**
```json
{ "success": true, "receipt_id": "FDREC-001", "updated_by": "treasurer@corp.com" }
```

---

### POST `/investment/fd/receipt/delete`
Soft-deletes receipts in `CAPTURED` or `REJECTED` status (creates a delete approval instance).

**Request**
```json
{
  "user_id":    "USR-001",
  "receipt_ids": ["FDREC-001"],
  "reason":     "Duplicate"
}
```

**Response `200`**
```json
{ "success": true, "results": [{ "receipt_id": "FDREC-001", "success": true }] }
```

---

### POST `/investment/fd/receipt/submit`
Submits a captured receipt for approval.

**Request**
```json
{ "user_id": "USR-001", "receipt_id": "FDREC-001" }
```

**Response `200`**
```json
{ "success": true, "receipt_id": "FDREC-001", "receipt_status": "APPROVAL_PENDING" }
```

**Side effects**
- Updates `receipt_status = 'APPROVAL_PENDING'`
- Async: `CreateInstance(FD_RECEIPT_APPROVE)`
- Async: `TriggerNotification` event `FD_RECEIPT_SUBMITTED`

---

### POST `/investment/fd/receipt/bulk-approve`
Bulk-approves receipts. Engine-first with direct-stamp fallback. Executes delete flip for approved DELETE requests.

**Request**
```json
{
  "user_id":     "USR-002",
  "receipt_ids": ["FDREC-001"],
  "comment":     "Matched with bank statement"
}
```

**Response `200`**
```json
{ "success": true, "approved_count": 1, "checker": "approver@corp.com" }
```

---

### POST `/investment/fd/receipt/bulk-reject`
Bulk-rejects receipts.

**Request**
```json
{
  "user_id":     "USR-002",
  "receipt_ids": ["FDREC-001"],
  "comment":     "Amount mismatch"
}
```

**Response `200`**
```json
{ "success": true, "rejected_count": 1, "checker": "approver@corp.com" }
```

---

### GET `/investment/fd/receipt/all`
Returns all receipts with latest audit row.

**Query params** `fd_id=FDMST-20240101-001` or `entity_id=ENT-0000001`

---

### GET `/investment/fd/receipt/detail`
Returns one receipt with full audit history.

**Query params** `receipt_id=FDREC-001`

---

### GET `/investment/fd/receipt/audit-history`
Returns audit history for a receipt.

**Query params** `receipt_id=FDREC-001`

---

### GET `/investment/fd/receipt/tds-register`
Returns the TDS register for an entity and period.

**Query params** `entity_id=ENT-0000001&period_start=2024-01-01&period_end=2024-03-31`

---

### POST `/investment/fd/receipt/reconcile/run`
Starts an asynchronous reconciliation run (matches receipts against bank statement entries).

**Request**
```json
{
  "user_id":       "USR-001",
  "entity_id":     "ENT-0000001",
  "entity_name":   "Corp Entity One",
  "period_start":  "2024-01-01",
  "period_end":    "2024-03-31",
  "matching_basis": "AMOUNT_DATE",
  "bank_id_filter": "BANK-001"
}
```

`matching_basis` values: `AMOUNT_DATE`, `AMOUNT_ONLY`, `DATE_ONLY`

**Response `200`** (immediate — reconciliation runs async)
```json
{
  "success":          true,
  "reconcile_run_id": "RECON-RUN-001",
  "run_status":       "RUNNING",
  "message":          "Reconciliation started. Poll /reconcile/status for results."
}
```

---

### GET `/investment/fd/receipt/reconcile/status`
Polls the status of a reconciliation run.

**Query params** `reconcile_run_id=RECON-RUN-001`

**Response** includes `run_status` (`RUNNING`, `COMPLETED`, `FAILED`), `matched_count`, `unmatched_count`, `exception_count`.

---

### GET `/investment/fd/receipt/reconcile/results`
Returns the full match/unmatch results of a completed reconciliation run.

**Query params** `reconcile_run_id=RECON-RUN-001`

---

### GET `/investment/fd/receipt/exceptions`
Returns all reconciliation exceptions (unmatched receipts requiring manual review).

**Query params** `entity_id=ENT-0000001`, `run_id=RECON-RUN-001` (optional)

---

### POST `/investment/fd/receipt/exceptions/resolve`
Marks an exception as `IN_REVIEW` (reviewer acknowledges it and begins working it).

**Request**
```json
{ "user_id": "USR-001", "exception_id": "FDEX-001", "comment": "Investigating" }
```

---

### POST `/investment/fd/receipt/exceptions/approve`
Approves a resolved exception (checker confirms the manual resolution is correct). Maker ≠ Checker enforced.

**Request**
```json
{ "user_id": "USR-002", "exception_id": "FDEX-001", "comment": "Verified against bank advice" }
```

**Response `200`**
```json
{ "success": true, "exception_id": "FDEX-001", "exception_status": "APPROVED" }
```

**Side effects**
- Async: `TriggerNotification` event `FD_RECEIPT_EXCEPTION_APPROVED`

---

### POST `/investment/fd/receipt/exceptions/close`
Closes an approved exception.

**Request**
```json
{ "user_id": "USR-002", "exception_id": "FDEX-001", "comment": "Closed post bank confirmation" }
```

---

### POST `/investment/fd/receipt/post-journals`
Posts accounting journal entries (debit bank / credit interest income / credit TDS payable) for approved receipts.

**Request**
```json
{
  "user_id":     "USR-001",
  "receipt_ids": ["FDREC-001"]
}
```

**Response `200`**
```json
{
  "success": true,
  "posted":  1,
  "skipped": 0,
  "results": [
    {
      "receipt_id":        "FDREC-001",
      "success":           true,
      "interest_entry_id": "JE-INT-001",
      "tds_entry_id":      "JE-TDS-001"
    }
  ]
}
```

---

### POST `/investment/fd/receipt/tds/update`
Updates TDS details on a receipt.

**Request**
```json
{
  "user_id":           "USR-001",
  "tds_id":            "TDS-001",
  "tds_amount":        18200,
  "tds_certificate_no": "TDS-CERT-001",
  "tds_date":          "2024-04-01"
}
```

---

### GET `/investment/fd/receipt/tds/detail`
Returns TDS detail for a receipt.

**Query params** `tds_id=TDS-001`

---

### GET `/investment/fd/receipt/tds/audit-history`
Returns TDS audit history.

**Query params** `tds_id=TDS-001`

---

## Module 6 — FD Interest & TDS Workbench (Read-only)

All four routes accept `entity_id` and optional `period_start` / `period_end` query parameters.

### GET `/investment/fd/workbench/interest-summary`
Returns a summary of interest received, TDS deducted, and net receipts grouped by FD and bank.

### GET `/investment/fd/workbench/tds-summary`
Returns TDS payable, paid, and outstanding balances by quarter/year.

### GET `/investment/fd/workbench/reconciliation-dashboard`
Returns the reconciliation status overview: matched, unmatched, and exception counts across all runs.

### GET `/investment/fd/workbench/interest-vs-accrual`
Compares accrued interest (from the accrual engine) against actually received interest (from receipts). Highlights under/over-receipt by FD.

**Query params** `entity_id=ENT-0000001&period_start=2024-01-01&period_end=2024-03-31`

**Response** Array per FD:
```json
[
  {
    "fd_id":              "FDMST-20240101-001",
    "fd_ref_no":          "SBI/FD/2024/0001",
    "total_accrued":      181250,
    "total_received":     163125,
    "difference":         18125,
    "difference_type":    "UNDER_RECEIPT",
    "tds_deducted":       18125
  }
]
```

---

## Error Response Format

All errors use this shape:
```json
{
  "success": false,
  "error":   "Human-readable error message"
}
```

| HTTP Status | Meaning |
|---|---|
| `400` | Bad request — missing/invalid fields |
| `401` | Session not found or expired |
| `403` | Forbidden — maker-checker violation |
| `404` | Record not found |
| `409` | Conflict — duplicate or invalid state transition |
| `500` | Internal server / database error |

---

## Approval Engine Summary

| Transaction Type | Trigger | Audit Table | PK Column |
|---|---|---|---|
| `FD_BOOKING` | Create booking | `investment.fd_audit_booking_request` | `booking_id` |
| `FD_BOOKING_CREATE` | Create booking (alias) | same | same |
| `FD_BOOKING_EDIT` | Update booking | same | same |
| `FD_BOOKING_DELETE` | Delete booking | same | same |
| `FD_CONFIRMATION_CREATE` | Capture confirmation | `investment.fd_audit_confirmation` | `confirmation_id` |
| `FD_CONFIRMATION_VARIANCE_RESOLVE` | Resolve threshold breach | same | same |
| `FD_MASTER_CREATE` | Activate FD | `investment.fd_audit_master` | `fd_id` |
| `FD_ACTIVATE` | Activate FD (alias) | same | same |
| `FD_ACCRUAL_APPROVE` | Submit accrual run | `investment.fd_accrual_run_audit` | `run_id` |
| `FD_RECEIPT_APPROVE` | Submit receipt | `investment.fd_interest_receipt_audit` | `receipt_id` |
| `FD_RECEIPT_EDIT` | Edit receipt | same | same |
| `FD_RECEIPT_DELETE` | Delete receipt | same | same |

---

## Notification Events Summary

| Route | Event |
|---|---|
| `/investment/fd/booking/create` | `FD_BOOKING_SUBMITTED` |
| `/investment/fd/booking/create-bulk` | `FD_BOOKING_SUBMITTED` (per booking) |
| `/investment/fd/booking/update` | `FD_BOOKING_EDIT_SUBMITTED` |
| `/investment/fd/booking/delete` | `FD_BOOKING_DELETE_SUBMITTED` (per booking) |
| `/investment/fd/booking/approve` | `FD_BOOKING_APPROVED` (per booking) |
| `/investment/fd/booking/reject` | `FD_BOOKING_REJECTED` (per booking) |
| `/investment/fd/booking/send-to-bank` | `FD_BOOKING_SENT_TO_BANK` |
| `/investment/fd/confirmation/capture` | `FD_CONFIRMATION_CAPTURED` |
| `/investment/fd/confirmation/resolve-variance` | `FD_CONFIRMATION_VARIANCE_RESOLVED` |
| `/investment/fd/confirmation/approve` | `FD_CONFIRMATION_APPROVED` (per confirmation) |
| `/investment/fd/confirmation/reject` | `FD_CONFIRMATION_REJECTED` (per confirmation) |
| `/investment/fd/confirmation/delete` | `FD_CONFIRMATION_DELETE_SUBMITTED` (per confirmation) |
| `/investment/fd/master/activate` | `FD_ACTIVATION_SUBMITTED` |
| `/investment/fd/master/approve` | `FD_ACTIVATION_APPROVED` (per FD) |
| `/investment/fd/master/reject` | `FD_ACTIVATION_REJECTED` (per FD) |
| `/investment/fd/accrual/run/submit` | `FD_ACCRUAL_RUN_SUBMITTED` |
| `/investment/fd/accrual/run/approve` | `FD_ACCRUAL_RUN_APPROVED` (per run) |
| `/investment/fd/accrual/run/reject` | `FD_ACCRUAL_RUN_REJECTED` (per run) |
| (scheduler auto-submit) | `FD_ACCRUAL_RUN_AUTO_SUBMITTED` |
| `/investment/fd/receipt/create` | `FD_RECEIPT_CAPTURED` |
| `/investment/fd/receipt/update` | `FD_RECEIPT_EDIT_SUBMITTED` |
| `/investment/fd/receipt/delete` | `FD_RECEIPT_DELETE_SUBMITTED` (per receipt) |
| `/investment/fd/receipt/submit` | `FD_RECEIPT_SUBMITTED` |
| `/investment/fd/receipt/bulk-approve` | `FD_RECEIPT_APPROVED` (per receipt) |
| `/investment/fd/receipt/bulk-reject` | `FD_RECEIPT_REJECTED` (per receipt) |
| `/investment/fd/receipt/post-journals` | `FD_RECEIPT_JOURNALS_POSTED` (per receipt) |
| `/investment/fd/receipt/reconcile/run` | `FD_RECONCILIATION_TRIGGERED` |
| `/investment/fd/receipt/exceptions/approve` | `FD_RECEIPT_EXCEPTION_APPROVED` |
