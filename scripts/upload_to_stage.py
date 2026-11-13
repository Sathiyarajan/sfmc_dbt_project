#!/usr/bin/env python3
"""Upload SFMC export JSON files to a Snowflake internal stage and load them
into the matching raw table via SP_LOAD_RAW.

Usage:
  python3 scripts/upload_to_stage.py --feed account_summary export1.json export2.json
  python3 scripts/upload_to_stage.py --feed journey_summary export.json --no-load

Requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD set as
environment variables (same convention as the dev target in profiles.yml).
"""
import argparse
import os
import sys

import snowflake.connector

FEEDS = {
    "account_summary": {
        "stage": "STG_EMAIL_ACCOUNT_SUMMARY",
        "raw_table": "RAW_EMAIL_ACCOUNT_SUMMARY",
    },
    "journey_summary": {
        "stage": "STG_EMAIL_JOURNEY_SUMMARY",
        "raw_table": "RAW_EMAIL_JOURNEY_SUMMARY",
    },
}

DATABASE = "SFMC_RAW_DEV"


def env(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Missing required env var: {name}")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--feed", choices=FEEDS, required=True)
    parser.add_argument("--no-load", action="store_true", help="Only PUT to stage, skip SP_LOAD_RAW")
    parser.add_argument("files", nargs="+", help="Local JSON export file(s) to upload")
    args = parser.parse_args()

    feed = FEEDS[args.feed]

    con = snowflake.connector.connect(
        account=env("SNOWFLAKE_ACCOUNT"),
        user=env("SNOWFLAKE_USER"),
        password=env("SNOWFLAKE_PASSWORD"),
        role="SFMC_DBT_DEV_ROLE",
        database=DATABASE,
        schema="PUBLIC",
        warehouse="SFMC_WH_DEV",
    )
    try:
        cur = con.cursor()
        for path in args.files:
            local_uri = f"file://{os.path.abspath(path)}"
            cur.execute(f"PUT {local_uri} @{feed['stage']} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
            status = cur.fetchall()[0][6]
            print(f"PUT {path} -> @{feed['stage']}: {status}")

        if not args.no_load:
            cur.execute(
                f"CALL {DATABASE}.PUBLIC.SP_LOAD_RAW("
                f"'{DATABASE}.PUBLIC.{feed['stage']}',"
                f"'{DATABASE}.PUBLIC.{feed['raw_table']}',"
                f"'{DATABASE}.PUBLIC.JSON_FF')"
            )
            print(" ", cur.fetchone()[0])
    finally:
        con.close()


if __name__ == "__main__":
    main()
