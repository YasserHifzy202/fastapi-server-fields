# Poultry Fields API (FastAPI)

FastAPI service that analyzes daily poultry Excel reports and returns two sections:
- **Operational** (ops)
- **Care** (care)
Each row includes a **Status**: `ERROR`, `NOTE`, or `OK` with **clear reasons**.

## Endpoints

- `GET /health` – simple health check.
- `POST /analyze` – accepts an Excel file (multipart field name: `file`) and returns:
  ```json
  {
    "ops":  [ { ... row with "Status" ... }, ... ],
    "care": [ { ... row with "Status" ... }, ... ]
  }
