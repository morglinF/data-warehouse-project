# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a complete data warehousing and analytics pipeline using **Dockerized PostgreSQL** and **Python**.  
It automates the **Medallion Architecture** flow — from raw ingestion (**Bronze**) to transformation (**Silver**) to analytical modeling (**Gold**) — following modern data engineering best practices.

---

## 🏗️ Data Architecture

The data architecture follows the **Medallion Architecture Model** with three key layers:

1. **Bronze Layer** – Raw ingestion: loads unmodified CSV data into the warehouse.  
2. **Silver Layer** – Cleansing and transformation: standardizes and prepares data for analytics.  
3. **Gold Layer** – Analytical modeling: exposes business-ready facts and dimensions.

---

## 🐳 Dockerized Data Warehouse Setup

This project runs fully inside Docker — with **PostgreSQL** as the database engine and a **Python ETL container** to run all scripts automatically.

### 🧩 Containers Overview

| Service | Description | Role |
|----------|--------------|------|
| `dw_db` | PostgreSQL 16 (Alpine) | Hosts the data warehouse and executes SQL scripts for all layers. |
| `dw_pipeline` | Python container | Runs the ETL pipeline (`elt.py`) to initialize schemas, execute scripts, and call stored procedures. |

---

### 🧱 Docker Compose Structure

```yaml
version: "3.9"

services:
  db:
    image: postgres:16-alpine
    container_name: dw_db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: dw
    ports:
      - "5432:5432"
    volumes:
      - dw_data:/var/lib/postgresql/data
      - ./datasets:/datasets:ro          # CSV datasets mounted here
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d dw"]
      interval: 5s
      timeout: 3s
      retries: 30
    restart: unless-stopped
    networks:
      - elt_network

  pipeline:
    build:
      context: ./elt
      dockerfile: Dockerfile
    container_name: dw_pipeline
    command: ["python", "elt.py"]
    environment:
      PGHOST: db
      PGPORT: "5432"
      POSTGRES_DB: dw
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: secret
    depends_on:
      db:
        condition: service_healthy
    restart: "no"
    networks:
      - elt_network
    volumes:
      - ./scripts:/app/scripts:ro
      - ./tests:/app/tests:ro
      - ./datasets:/app/datasets:ro

volumes:
  dw_data:

networks:
  elt_network:
    driver: bridge

```

### ⚙️ Automatic ETL Pipeline (elt.py)

The pipeline container runs automatically after PostgreSQL is healthy.
elt.py performs these steps in order:

- Runs /scripts/init_database.sql — creates the bronze, silver, and gold schemas.
- Executes the DDL scripts for each layer:
- bronze/ddl_bronze.sql
- silver/ddl_silver.sql
- gold/ddl_gold.sql

Loads stored procedures:

- bronze/proc_load_bronze.sql
- silver/proc_load_silver.sql

Calls the ETL procedures:
- CALL bronze.load_bronze();
- CALL silver.load_silver();

When the pipeline finishes, your Bronze and Silver tables are populated, and Gold views are ready for queries.

### 📦 Folder & File Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw CSV data (mounted into Postgres)
│   ├── source_crm/
│   └── source_erp/
│
├── scripts/                            # SQL DDLs and procedures
│   ├── init_database.sql
│   ├── bronze/
│   │   ├── ddl_bronze.sql
│   │   ├── proc_load_bronze.sql
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   ├── proc_load_silver.sql
│   ├── gold/
│       ├── ddl_gold.sql
│
├── elt/
│   ├── Dockerfile                      # Builds the pipeline container
│   ├── elt.py                          # Orchestration script (runs SQL in order)
│
├── tests/                              # SQL test scripts for validation
│   ├── quality_check_silver.sql
│   ├── quality_check_gold.sql
│
├── docker-compose.yaml                 # Defines services and networking
├── README.md                           # This file
└── requirements.txt
```
### 🚀 Running the Project
  1️⃣ Start all containers
  docker compose up -d --build
  
  2️⃣ Watch logs
  docker logs -f dw_pipeline
  
  
  You’ll see:
  ```
  [pipeline] ==> /app/scripts/init_database.sql
  [pipeline] ==> /app/scripts/bronze/ddl_bronze.sql
  [pipeline] ==> CALL bronze.load_bronze();
  [pipeline] ==> CALL silver.load_silver();
```
  
  3️⃣ Connect to Postgres (psql)
  docker exec -it dw_db psql -U postgres -d dw


### Orchestrate ETL outside the database — in elt.py, not inside SQL files.

Separate DDLs (structure) from DML/ETL (data movement).

Use COPY FROM with mounted datasets (/datasets/...).

Log clearly — pipeline prints every script it executes.

Use schema-qualified names (bronze., silver., gold.*).

🧰 Local Development Workflow

A standard workflow for rebuilding, re-running ETL, and validating data quality:

🔁 Rebuild the environment from scratch
```
docker compose down -v
docker compose up -d --build
```


This wipes existing volumes and ensures all scripts (init + DDL + procedures + ETL) run cleanly.

🧪 Run ETL again manually (without rebuilding)
```
docker exec -it dw_db psql -U postgres -d dw -c "CALL bronze.load_bronze();"
docker exec -it dw_db psql -U postgres -d dw -c "CALL silver.load_silver();"
```

Removes all containers and data volumes — useful before a fresh test run.

### 📊 Outcome

After successful run:

Bronze Layer: Raw CRM + ERP CSV data ingested.

Silver Layer: Cleaned, validated, standardized.

Gold Layer: Analytical-ready views (dim_customers, dim_products, fact_sales).
