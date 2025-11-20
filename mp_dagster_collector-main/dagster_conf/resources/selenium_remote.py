from dagster import resource, Field
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

@resource(
    config_schema={
        "grid_url": Field(
            str,
            is_required=True,
            description="URL Selenium Grid",
        ),
        "chrome_profiles": Field(
            dict,
            is_required=False,
            default_value={
                "1":    "Profile WB inter",
                "2":    "Profile WB ut",
                "3":    "Profile WB kravchik",
                "4":    "Profile WB pomazanova",
                "5":    "Profile WB avangard",
                "45":   "Profile WB petflat"
            },
            description="Словарь строковых token_id → имени папки профиля внутри /google_chrome_users/",
        ),
    }
)
def selenium_remote(init_context):
    """
    Ресурс, возвращающий функцию get_driver(token_id),
    которая создаёт Remote WebDriver, используя указанный профиль Chrome.
    Также к самой функции привязан атрибут `.profiles` с маппингом.
    """
    grid_url = init_context.resource_config["grid_url"]
    profiles = init_context.resource_config["chrome_profiles"]

    def get_driver(token_id: int) -> webdriver.Remote:
        profile_name = profiles.get(str(token_id))
        init_context.log.info(f"[selenium_remote] token_id={token_id!r} → profile_name={profile_name!r}")
        if not profile_name:
            raise ValueError(f"No Chrome profile configured for token_id={token_id}")

        opts = Options()
        opts.add_argument("--user-data-dir=/google_chrome_users/")
        opts.add_argument(f"--profile-directory={profile_name}")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")

        return webdriver.Remote(command_executor=grid_url, options=opts)

    get_driver.profiles = profiles

    return get_driver
