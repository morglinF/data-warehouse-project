import os
import sys
import time
from pathlib import Path
import subprocess
import psycopg

# --- Connection settings from environment ---
PGHOST = os.environ.get("PGHOST", "localhost")
PGPORT = int(os.environ.get("PGPORT", "5432"))
PGDB   = os.environ.get("POSTGRES_DB", "dw")
PGUSER = os.environ.get("POSTGRES_USER", "postgres")
PGPASS = os.environ.get("POSTGRES_PASSWORD", "")

ROOT   = Path("/app")
SCRIPTS = ROOT / "scripts"        
TESTS  = ROOT / "tests"

def wait_for_db(timeout=180):
    start = time.time()
    while True:
        try:
            with psycopg.connect(
                host=PGHOST, port=PGPORT, dbname=PGDB, user=PGUSER, password=PGPASS,
                connect_timeout=3,
            ):
                print(f"[pipeline] DB ready at {PGHOST}:{PGPORT}/{PGDB} as {PGUSER}")
                return
        except Exception as e:
            if time.time() - start > timeout:
                print(f"[pipeline] ERROR: DB not ready after {timeout}s\n{e}", flush=True)
                sys.exit(1)
            time.sleep(1)

def run_sql(path: Path):
    print(f"[pipeline] ==> {path}")
    sql = path.read_text(encoding="utf-8")
    # autocommit so DDL/SPs/transactions inside scripts work as written
    with psycopg.connect(
        host=PGHOST, port=PGPORT, dbname=PGDB, user=PGUSER, password=PGPASS, autocommit=True
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def run_py(path: Path):
    # Minimal: execute the Python script in a subprocess.
    # It can import psycopg and read the same PG* env vars already present.
    print(f"[pipeline] ==> {path} (python)")
    res = subprocess.run([sys.executable, str(path)], cwd=str(path.parent))
    if res.returncode != 0:
        raise RuntimeError(f"Python step failed: {path} (code {res.returncode})")

def must_exist(path: Path):
    if not path.exists():
        print(f"[pipeline] MISSING: {path}", flush=True)
        sys.exit(1)

def main():
    # Sanity
    must_exist(SCRIPTS)
    wait_for_db()

    # 1) Init database (schemas/roles/extensions/search_path; DO NOT create database here)
    init_sql = SCRIPTS / "init_database.sql"
    must_exist(init_sql)
    run_sql(init_sql)

    # 2) Bronze (strict order: DDL -> proc/load)
    bronze_dir = SCRIPTS / "bronze"
    must_exist(bronze_dir)
    run_sql(bronze_dir / "ddl_bronze.sql")
    run_sql(bronze_dir / "proc_load_bronze.sql")

    # 3) Silver (DDL -> proc/load)
    silver_dir = SCRIPTS / "silver"
    must_exist(silver_dir)
    run_sql(silver_dir / "ddl_silver.sql")
    run_sql(silver_dir / "proc_load_silver.sql")

    # 4) Gold
    gold_dir = SCRIPTS / "gold"
    must_exist(gold_dir)
    gold_sql = gold_dir / "ddl_gold.sql"
    gold_py  = gold_dir / "ddl_gold.py"

    if gold_sql.exists():
        run_sql(gold_sql)
    elif gold_py.exists():
        # You provided ddl_gold.py — we execute it. It should use the same env vars for DB access.
        run_py(gold_py)
    else:
        print("[pipeline] WARNING: No gold DDL found (expected ddl_gold.sql or ddl_gold.py)")

    # 5) Tests (silver first, then gold)
    tests_dir = ROOT / "tests"
    must_exist(tests_dir)
    silver_test = tests_dir / "quality_check_silver.sql"
    gold_test   = tests_dir / "quality_check_gold.sql"
    # Fail fast if any raises exception
    if silver_test.exists(): run_sql(silver_test)
    if gold_test.exists():   run_sql(gold_test)

    run_sql(SCRIPTS / "run_calls.sql")

    print("[pipeline] ✅ All steps completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[pipeline] ❌ ERROR: {e}", flush=True)
        sys.exit(1)