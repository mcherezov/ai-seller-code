from dagster import job, op
import subprocess
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

@op
def run_llm_analyzer_script(context):
    script_path = Path(__file__).resolve().parent.parent.parent / "llm_analyzer.py"

    context.log.info(f"Running script at: {script_path}")

    env = os.environ.copy()

    result = subprocess.run(
        ["python", str(script_path)],
        capture_output=True,
        text=True,
        env=env
    )

    context.log.info("Script STDOUT:\n" + result.stdout)
    context.log.warning("Script STDERR:\n" + result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Script failed with exit code {result.returncode}")


@job
def llm_wrapper_job():
    run_llm_analyzer_script()
