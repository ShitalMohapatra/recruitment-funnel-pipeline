"""Recruitment funnel pipeline: raw -> clean -> DuckDB -> funnel -> quality checks."""
import os
import random
import shutil
from datetime import datetime, timedelta

import duckdb
import pandas as pd

RAW_FILE = "raw_candidates.csv"
RAW_COPY = "data/raw/raw_candidates.csv"
CLEAN_FILE = "data/clean/clean_candidates.csv"
DB_FILE = "candidates.duckdb"


def create_raw_data():
    """Step 1: create the messy fake data."""
    random.seed(42)
    roles = ["Data Analyst", "Senior Data Engineer", "AI Engineer", "SaaS Account Executive", "Sales Manager"]
    clients = ["Acme Corp", "Nimbus Tech", "DataWorks", "Orbit Labs"]
    sources = ["LinkedIn", "Referral", "Job Portal", "Database"]
    stages = ["Interview 1", "interview1", "Interview 2", "Final Round", "final round"]
    date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y"]

    rows = []
    start = datetime(2026, 5, 1)
    for i in range(1, 193):
        sent = start + timedelta(days=random.randint(0, 120))
        shortlisted_flag = random.random() < 0.5
        stage = random.choice(stages) if shortlisted_flag and random.random() < 0.6 else None
        offer = random.choice(["Offered", "Offered", "Rejected"]) if stage and random.random() < 0.5 else None
        joined = random.choice(["Yes", "No"]) if offer == "Offered" else None
        drop_reason = random.choice(["Salary", "Counter offer", "Notice period"]) if shortlisted_flag and joined != "Yes" and random.random() < 0.1 else None
        rows.append({
            "candidate_id": f"C{i:03d}",
            "role": random.choice(roles),
            "client": random.choice(clients),
            "source": random.choice(sources),
            "sent_date": sent.strftime(random.choice(date_formats)),
            "shortlisted": random.choice(["Yes", "yes", "Y"]) if shortlisted_flag else random.choice(["No", None]),
            "interview_stage": stage,
            "offer_status": offer,
            "joined": joined,
            "drop_reason": drop_reason,
        })

    df = pd.DataFrame(rows)
    df = pd.concat([df, df.sample(8, random_state=1)]).sample(frac=1, random_state=2)
    df.to_csv(RAW_FILE, index=False)
    return len(df)


def save_raw_copy():
    """Step 3: keep an untouched raw copy."""
    os.makedirs("data/raw", exist_ok=True)
    shutil.copy(RAW_FILE, RAW_COPY)


def clean_data():
    """Step 4: clean a copy of the raw data and save it."""
    df = pd.read_csv(RAW_COPY)
    clean = df.copy()

    clean = clean.drop_duplicates(subset="candidate_id")

    clean["shortlisted"] = (clean["shortlisted"].str.strip().str.lower()
                            .map({"yes": "Yes", "y": "Yes", "no": "No"})
                            .fillna("Unknown"))

    clean["interview_stage"] = (clean["interview_stage"].str.strip().str.lower()
                                .map({"interview 1": "Interview 1", "interview1": "Interview 1",
                                      "interview 2": "Interview 2", "final round": "Final Round"})
                                .fillna("Unknown"))

    d1 = pd.to_datetime(clean["sent_date"], format="%Y-%m-%d", errors="coerce")
    d2 = pd.to_datetime(clean["sent_date"], format="%d/%m/%Y", errors="coerce")
    d3 = pd.to_datetime(clean["sent_date"], format="%d-%b-%Y", errors="coerce")
    clean["sent_date_clean"] = d1.fillna(d2).fillna(d3)

    os.makedirs("data/clean", exist_ok=True)
    clean.to_csv(CLEAN_FILE, index=False)
    return len(clean)


def load_to_duckdb():
    """Step 5: load the clean file into a DuckDB table."""
    con = duckdb.connect(DB_FILE)
    con.execute(f"""
        CREATE OR REPLACE TABLE candidates AS
        SELECT * FROM read_csv_auto('{CLEAN_FILE}')
    """)
    return con


def build_funnel(con):
    """Step 6c: save the funnel as a final (gold) table."""
    con.execute("""
        CREATE OR REPLACE TABLE funnel_summary AS
        WITH f AS (
            SELECT
                COUNT(*) AS sourced,
                SUM(CASE WHEN shortlisted = 'Yes' THEN 1 ELSE 0 END) AS screened,
                SUM(CASE WHEN interview_stage <> 'Unknown' THEN 1 ELSE 0 END) AS interviewed,
                SUM(CASE WHEN offer_status = 'Offered' THEN 1 ELSE 0 END) AS offered,
                SUM(CASE WHEN joined = 'Yes' THEN 1 ELSE 0 END) AS hired
            FROM candidates
        )
        SELECT 1 AS stage_order, 'Sourced' AS stage, sourced AS candidates,
               CAST(NULL AS DOUBLE) AS pct_of_previous FROM f
        UNION ALL
        SELECT 2, 'Screened', screened, ROUND(100.0 * screened / sourced, 1) FROM f
        UNION ALL
        SELECT 3, 'Interviewed', interviewed, ROUND(100.0 * interviewed / screened, 1) FROM f
        UNION ALL
        SELECT 4, 'Offered', offered, ROUND(100.0 * offered / interviewed, 1) FROM f
        UNION ALL
        SELECT 5, 'Hired', hired, ROUND(100.0 * hired / offered, 1) FROM f
    """)


def run_quality_checks(con):
    """Step 7: stop the pipeline if the data is wrong."""
    checks = {
        "No duplicate candidate IDs":
            "SELECT COUNT(*) - COUNT(DISTINCT candidate_id) FROM candidates",
        "No blank dates":
            "SELECT COUNT(*) FROM candidates WHERE sent_date_clean IS NULL",
        "No dates in the future":
            "SELECT COUNT(*) FROM candidates WHERE sent_date_clean > CURRENT_DATE",
        "Shortlisted is only Yes / No / Unknown":
            "SELECT COUNT(*) FROM candidates WHERE shortlisted NOT IN ('Yes', 'No', 'Unknown')",
        "Joined only if Offered":
            "SELECT COUNT(*) FROM candidates WHERE joined = 'Yes' AND (offer_status IS NULL OR offer_status <> 'Offered')",
        "Funnel never grows":
            "SELECT COUNT(*) FROM funnel_summary WHERE pct_of_previous > 100",
    }
    failed = 0
    for name, sql in checks.items():
        bad_rows = con.execute(sql).fetchone()[0]
        status = "PASS" if bad_rows == 0 else "FAIL"
        if bad_rows != 0:
            failed += 1
        print(f"    {status}  {name}  (problem rows: {bad_rows})")
    if failed > 0:
        raise Exception(f"{failed} data quality check(s) failed")


def main():
    print("1/6 Creating raw data")
    print("    rows created:", create_raw_data())

    print("2/6 Saving untouched raw copy")
    save_raw_copy()

    print("3/6 Cleaning")
    print("    clean rows:", clean_data())

    print("4/6 Loading into DuckDB")
    con = load_to_duckdb()
    print("    rows in table:", con.execute("SELECT COUNT(*) FROM candidates").fetchone()[0])

    print("5/6 Building funnel table")
    build_funnel(con)

    print("6/6 Running quality checks")
    run_quality_checks(con)

    print("Pipeline finished successfully")
    con.close()


if __name__ == "__main__":
    main()
