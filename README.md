# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository! 🚀  
This project demonstrates a complete data warehousing and analytics pipeline using **Dockerized PostgreSQL** and **Python**.  
It automates the **Medallion Architecture** flow — from raw ingestion (**Bronze**) to transformation (**Silver**) to analytical modeling (**Gold**) — following modern data engineering best practices.

---

## 🏗️ Data Architecture

The data architecture follows the **Medallion Architecture Model** with three key layers:

![Data Architecture](docs/data_architecture.png)

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
