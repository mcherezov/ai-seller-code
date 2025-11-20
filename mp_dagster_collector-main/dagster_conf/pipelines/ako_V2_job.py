from dagster import job, op
import subprocess
from pathlib import Path
import os


@op
def run_ako_V2_script(context):
    # Путь к скрипту
    script_path = Path(__file__).resolve().parent.parent.parent / "keywords_opt" / "OptManager.py"

    # Корень проекта — /opt/mp_data_collector
    project_root = Path(__file__).resolve().parent.parent.parent
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    context.log.info(f"Running script at: {script_path}")
    context.log.info(f"PYTHONPATH: {project_root}")

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
def ako_V2_wrapper_job():
    run_ako_V2_script()
