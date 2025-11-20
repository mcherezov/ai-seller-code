from dagster import schedule
from dagster_conf.pipelines.www_cashback_reports_job import wb_www_cashback_job

@schedule(
    cron_schedule="45 9 * * MON",  # каждый понедельник в 09:45 по МСК
    job=wb_www_cashback_job,
    execution_timezone="Europe/Moscow",
)
def wb_www_cashback_schedule():
    return {
        "resources": {
            "selenium_remote": {
                "config": {
                    "grid_url": "http://158.160.57.59:4444/wd/hub",
                    "chrome_profiles": {
                        "1": "Profile WB inter",
                        "2": "Profile WB ut",
                        "3": "Profile WB kravchik",
                        "4": "Profile WB pomazanova",
                        "5": "Profile WB avangard",
                        "45": "Profile WB petflat"
                    },
                }
            }
        }
    }
