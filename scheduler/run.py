import os
import sys
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from subprocess import run, CalledProcessError
from ingestion.config import SCHEDULE_CRON

def run_step(cmd):
    try:
        print(f"[scheduler] Running: {cmd}")
        r = run(cmd, check=True)
        print(f"[scheduler] Completed: {cmd} -> rc={r.returncode}")
    except CalledProcessError as e:
        print(f"[scheduler][ERROR] {cmd} failed rc={e.returncode}", file=sys.stderr)

def batch_pipeline():
    run_step([sys.executable, "/app/ingestion/batch_ingest.py"])
    run_step([sys.executable, "/app/processing/clean_sanctions.py"])
    run_step([sys.executable, "/app/warehouse/load_to_duckdb.py"])

if __name__ == "__main__":
    sched = BlockingScheduler(timezone="UTC")

    minute, hour, day, month, dow = SCHEDULE_CRON.split()
    trigger = CronTrigger(minute=minute, hour=hour, day=day, month=month, day_of_week=dow)
    sched.add_job(batch_pipeline, trigger, id="daily_batch", replace_existing=True)
    print(f"[scheduler] Scheduled daily batch with cron '{SCHEDULE_CRON}' (UTC).")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("[scheduler] Shutting down...")
