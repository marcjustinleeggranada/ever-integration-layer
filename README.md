# EVER Integration Layer

Prototype of the integration layer from the INTE 301 midterm case study. It sits between the legacy branch POS (Point of Sale) at three EVER Supermarket branches (Sucat, Zapote, 4th Estate) and SAP Business One.

The production design in the document uses MuleSoft Anypoint. This prototype runs the same flows in plain Node.js so the behavior can be demonstrated and tested without a MuleSoft licence. It has no dependencies and needs Node 20 or newer.

## Run it

    npm start        # http://localhost:3000
    npm test         # 13 end-to-end checks, starts its own server

Environment variables: `PORT`, `RETRY_SCALE` (1 is the real retry schedule, default 0.1 runs ten times faster), `RATE_LIMIT` (requests per minute per client, default 600).

## What is in one process

| Part | Where | What it does |
|---|---|---|
| API gateway | `/oauth/token`, `/v1/*` | OAuth 2.0 client credentials per branch agent, rate limit, input validation |
| Queue and worker | in memory | Retry with exponential backoff (5s, 30s, 2m, 10m, 30m), circuit breaker, dead-letter queue, review queue |
| Transformation | `transformSale` | Legacy SKU to ERP item code, weighed-item barcode parsing, 12:00 AM business-day cutoff |
| Mock ERP | `/sap/b1s/v1/*` | Stand-in for the SAP Business One Service Layer: Invoices, InventoryPostings, Items, Login |
| Simulator | `/sim/*` | Plays the branch sync agents and the ERP team so each test case can be run from the dashboard |
| Dashboard | `/` | Live view of queue, ERP state, messages, dead-letter and review queues, reconciliation, price changes |

State lives in memory and resets when the process restarts.

## Gateway routes

All `/v1` routes need `Authorization: Bearer <token>`. Get a token with `POST /oauth/token` and a JSON body of `grant_type`, `client_id`, `client_secret`.

| Route | Who | Purpose |
|---|---|---|
| `POST /v1/sales` | branch agent | Batch of sales, flow F1. Returns 202 with a status per sale: accepted, duplicate, or rejected |
| `POST /v1/stock-movements` | branch agent | Receipts, transfers, adjustments, flow F2 |
| `POST /v1/prices` | ERP | Approve a price change, flow F3 |
| `GET /v1/price-updates` and `POST /v1/price-updates/:id/ack` | branch agent | Fetch pending price changes and acknowledge them |
| `POST /v1/reconciliation` | branch agent | End-of-day file as JSON rows or CSV text, flow F6. Returns variance and a status of OK or FLAG |

Demo credentials (change them with `SECRET_SUCAT`, `SECRET_ZAPOTE`, `SECRET_4TH`, `SECRET_ADMIN`):

| Client | Secret |
|---|---|
| `agent-sucat` | `sucat-demo-secret` |
| `agent-zapote` | `zapote-demo-secret` |
| `agent-4thestate` | `4thestate-demo-secret` |
| `erp-admin` | `erp-demo-secret` |

A sale looks like this:

    {
      "sales": [{
        "terminal": "POS18-S311",
        "invoice_no": "00-123456",
        "pos_transaction_no": "00-123457",
        "timestamp": "2026-11-15T03:20:00Z",
        "tender_type": "Credit Card",
        "approval_code": "482913",
        "lines": [ { "sku": "MLK-0100", "qty": 2 }, { "barcode": "2000042135003" } ]
      }]
    }

The idempotency key is the branch, terminal and invoice number. A weighed-item barcode is 13 digits starting with 2: item code (6 digits), price in centavos (5 digits), check digit. The layer divides the price by the current price per kilo to get the weight.

## Test cases covered

| Case | Check |
|---|---|
| TC1 | Barcode price 135.00 at 450.00 per kilo deducts 0.30 kg from the earliest-expiry batch |
| TC2 | Same invoice sent 3 times posts once, 2 duplicates acknowledged |
| TC3 | 500 sales buffered in an outage arrive once each, all within 5 minutes |
| TC4 | A price change is acknowledged by all 3 branches |
| TC5 | An unmapped SKU goes to the review queue, not the ERP |
| TC6 | Sales at 11:59:59 PM and 12:00:01 AM land on different business days |
| TC7 | A debit sale labelled "Credit Card" keeps its approval code, and reconciliation matches on it |

The suite also checks gateway sign-in, input validation, reconciliation variance, ERP session expiry with one re-login, the circuit breaker, and the dead-letter queue with manual replay.

## What is simulated

- The ERP is a mock with five items and three branches. It has no real SAP behavior beyond the document shapes used here.
- The queue is in memory. A production build would use a durable broker.
- The SFTP end-of-day file is sent as an HTTPS upload to `/v1/reconciliation`.
- Loyalty, supplier ordering, and Maya, GCash and card payments are outside this prototype.
