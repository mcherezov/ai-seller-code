import os
from dagster import job, op
import subprocess
from pathlib import Path


@op
def run_ucb_bandit_script(context):
    script_path = Path(__file__).resolve().parent.parent.parent / "bandit" / "run.py"

    context.log.info(f"Running UCB Bandit script at: {script_path}")

    result = subprocess.run(
        ["python", str(script_path)],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": "."}
    )

    context.log.info("Script STDOUT:\n" + result.stdout)
    context.log.warning("Script STDERR:\n" + result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Script failed with exit code {result.returncode}")


@job
def ucb_bandit_wrapper_job():
    run_ucb_bandit_script()
