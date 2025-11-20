from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import json, time, re
import requests

# Selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    StaleElementReferenceException,
    TimeoutException,
    MoveTargetOutOfBoundsException,
)
from selenium.webdriver import ActionChains
try:
    from zoneinfo import ZoneInfo
    MSK = ZoneInfo("Europe/Moscow")
except Exception:
    MSK = None

SALE_REPORT_LIST_URL = "https://seller.wildberries.ru/suppliers-mutual-settlements/reports-implementations/reports-daily"
SALE_REPORT_DETAIL_URL = (
    "https://seller.wildberries.ru/suppliers-mutual-settlements/"
    "reports-implementations/reports-daily/report/{report_id}?isGlobalBalance=false"
)

# ----------------------------- helpers -----------------------------

def _now_tz(context) -> datetime:
    """Отдаём таймштамп с TZ: приоритет — postgres.tz, затем Europe/Moscow, затем UTC."""
    tz = getattr(getattr(context.resources, "postgres", None), "tz", None) or MSK or timezone.utc
    return datetime.now(tz=tz)

def _wait_dom_ready(driver, timeout: int = 30):
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def _set_date_input_js(driver, elem, date_str: str):
    driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input',{bubbles:true}));", elem, date_str)
    try:
        elem.clear()
        elem.send_keys(date_str)
    except Exception:
        pass

def _click_first_found(driver, selectors: List[str]) -> bool:
    for sel in selectors:
        try:
            el = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            el.click()
            return True
        except Exception:
            continue
    # попробуем :contains текстом (через XPath)
    text_variants = ["Применить", "Сохранить", "Показать", "Найти", "Сформировать", "Обновить"]
    for txt in text_variants:
        try:
            el = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{txt}')]"))
            )
            el.click()
            return True
        except Exception:
            continue
    return False

def _set_date_range(driver, date_str: str):
    """
    Пытаемся выставить обе даты = date_str.
    Перебираем распространённые селекторы WB.
    """
    candidates = [
        # пара input-ов "с/по"
        ("input[name='dateFrom']", "input[name='dateTo']"),
        ("input[placeholder='ДД.ММ.ГГГГ']", "input[placeholder='ДД.ММ.ГГГГ']"),
        ("input[type='date']", "input[type='date']"),
    ]
    from_el = to_el = None
    for cs_from, cs_to in candidates:
        try:
            _from = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, cs_from)))
            _to   = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, cs_to)))
            from_el, to_el = _from, _to
            break
        except Exception:
            continue
    if from_el is None or to_el is None:
        # Возможно, уже применён фильтр по нужной дате — просто идём дальше
        return

    _set_date_input_js(driver, from_el, date_str)
    _set_date_input_js(driver, to_el,   date_str)

    # нажимаем кнопку применения/сохранения
    _click_first_found(driver, [
        "button[type='submit']",
        "button.btn-primary",
        "button.wb-button--primary",
    ])
    # ждём перерисовку
    time.sleep(1.0)

def _extract_auth_token(driver) -> Optional[str]:
    """
    Достаём x-supplier-auth-v3 из localStorage/sessionStorage (чаще лежит в LS).
    Возвращаем строку либо None.
    """
    js = """
    try {
      const keys = [
        'X-Supplier-Auth-v3', 'x-supplier-auth-v3',
        'XSupplierAuthV3', 'supplierAuth', 'authv3'
      ];
      for (const k of keys) {
        const v1 = window.localStorage.getItem(k);
        if (v1) return v1;
        const v2 = window.sessionStorage.getItem(k);
        if (v2) return v2;
      }
      // Попробуем найти по маске
      for (let i=0; i<window.localStorage.length; i++){
        const k = window.localStorage.key(i);
        if (!k) continue;
        const low = k.toLowerCase();
        if (low.includes('supplier') && low.includes('auth')) {
          const v = window.localStorage.getItem(k);
          if (v) return v;
        }
      }
      return null;
    } catch(e){ return null; }
    """
    try:
        tok = driver.execute_script(js)
        if tok and isinstance(tok, str) and len(tok) > 10:
            return tok
    except Exception:
        pass
    return None

def _collect_report_ids_from_dom(driver, date_str: str) -> List[str]:
    """
    Ищем в таблице все ссылки вида .../report/<id>, дополнительно фильтруем по дате.
    Если нет явной колонки с датой — берём все id (обычно на день формируется 1–3 отчёта).
    """
    ids: List[str] = []
    anchor_elems = driver.find_elements(By.CSS_SELECTOR, "a[href*='/reports-daily/report/']")
    for a in anchor_elems:
        try:
            href = a.get_attribute("href") or ""
            m = re.search(r"/report/(\d+)", href)
            if not m:
                continue
            rid = m.group(1)

            # Пытаемся проверить дату по строке <tr>
            tr = a.find_element(By.XPATH, "./ancestor::tr")
            tr_text = (tr.text or "").strip()
            if date_str in tr_text:
                ids.append(rid)
                continue

            ids.append(rid)
        except Exception:
            continue

    # Дедуп, порядок как на странице
    seen = set()
    out = []
    for r in ids:
        if r in seen:
            continue
        seen.add(r)
        out.append(r)
    return out

def _cookies_dict(driver) -> Dict[str, str]:
    try:
        return {c["name"]: c["value"] for c in (driver.get_cookies() or []) if "name" in c and "value" in c}
    except Exception:
        return {}


def _open_filter_panel(driver) -> None:
    """
    Делает всё, чтобы панель фильтра с датами была раскрыта.
    Идём по нескольким селекторам, считаем успехом наличие полей start/end.
    """
    # Если поля уже в DOM — ок
    if driver.find_elements(By.XPATH, '//*[@id="startDate"]') or driver.find_elements(By.NAME, "dateFrom"):
        return

    candidates = [
        # твой старый XPATH:
        (By.XPATH, '//*[@id="app-content-id"]/div[1]/div/div/div/div[3]/div/div[1]/div/div[1]/div[2]/div/div/button'),
        # более общие варианты:
        (By.CSS_SELECTOR, "button[data-testid*='filter'], button[aria-controls*='filter']"),
        (By.XPATH, "//button[contains(., 'Фильтр') or contains(., 'Период') or contains(., 'Фильтры')]"),
    ]

    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            try:
                btn.click()
            except (ElementClickInterceptedException, MoveTargetOutOfBoundsException):
                driver.execute_script("arguments[0].click();", btn)
            # появление любого поля даты — успех
            WebDriverWait(driver, 5).until(
                lambda d: d.find_elements(By.XPATH, '//*[@id="startDate"]')
                or d.find_elements(By.NAME, "dateFrom")
                or d.find_elements(By.CSS_SELECTOR, "input[placeholder='ДД.ММ.ГГГГ']")
            )
            return
        except Exception:
            continue
    # если не смогли открыть — не падаем, дальше попробуем найти поля напрямую


def _wait_date_popup_open(driver, elem, timeout=3) -> bool:
    """
    Ждём открытый попап календаря или хотя бы фокус на инпуте.
    """
    def _probe(d):
        try:
            opened = d.execute_script(
                """
                const el = arguments[0];
                const focused = document.activeElement === el;
                const popup = document.querySelector(
                  "div[role='dialog'] .calendar, .react-datepicker, .wb-calendar, .datepicker, div[class*='DatePicker'], div[class*='Calendar']"
                );
                return Boolean(focused || popup);
                """,
                elem,
            )
            return bool(opened)
        except Exception:
            return False

    try:
        WebDriverWait(driver, timeout).until(lambda d: _probe(d))
        return True
    except TimeoutException:
        return False


def _open_date_input(driver, elem) -> None:
    """
    Надёжно открывает календарь для данного input:
    скролл → обычный клик → JS-клик → клик по иконке → фокус+клавиши.
    """
    if elem is None:
        return
    for attempt in range(4):
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
            WebDriverWait(driver, 5).until(EC.element_to_be_clickable(elem))
        except Exception:
            pass

        # 1) обычный клик
        try:
            elem.click()
        except (ElementClickInterceptedException, StaleElementReferenceException, MoveTargetOutOfBoundsException):
            # 2) JS-клик
            try:
                driver.execute_script("arguments[0].click();", elem)
            except Exception:
                pass

        if _wait_date_popup_open(driver, elem, timeout=2):
            return

        # 3) клик по иконке календаря рядом с input
        try:
            icon = None
            # соседняя кнопка/иконка
            try:
                icon = elem.find_element(By.XPATH, "./following-sibling::button|./parent::*/following-sibling::button")
            except Exception:
                pass
            if not icon:
                icon = elem.find_element(By.XPATH, "./ancestor::*[self::div or self::label][1]//button")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", icon)
            try:
                icon.click()
            except Exception:
                driver.execute_script("arguments[0].click();", icon)
        except Exception:
            pass

        if _wait_date_popup_open(driver, elem, timeout=2):
            return

        # 4) фокус + клавиши, которые часто открывают календарь
        try:
            driver.execute_script("arguments[0].focus();", elem)
            ActionChains(driver).move_to_element(elem).click(elem).perform()
            elem.send_keys(Keys.SPACE)
            if _wait_date_popup_open(driver, elem, timeout=1):
                return
            elem.send_keys(Keys.ARROW_DOWN)
            if _wait_date_popup_open(driver, elem, timeout=1):
                return
            elem.send_keys(Keys.ENTER)
            if _wait_date_popup_open(driver, elem, timeout=1):
                return
        except Exception:
            pass

# ----------------------------- public API -----------------------------

async def get_report_jobs(context, business_dttm: datetime, company_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает список заданий [{report_id, cookies, headers, report_date}], полностью
    повторяя DOM-логику старой реализации: клик по кнопке фильтра, XPATH-поля дат,
    «Сохранить», чтение строк таблицы (div Reports-table-row__).
    """
    driver = context.resources.selenium_remote(company_id)

    tz = getattr(context.resources.selenium_remote, "tz", None) or MSK or timezone.utc
    date_str = business_dttm.astimezone(tz).strftime("%d.%m.%Y")

    try:
        driver.get(SALE_REPORT_LIST_URL)
        _wait_dom_ready(driver, timeout=45)

        _open_filter_panel(driver)

        start_field = None
        end_field = None
        find_candidates = [
            (By.XPATH, '//*[@id="startDate"]'), (By.XPATH, '//*[@id="endDate"]'),
            (By.NAME, "dateFrom"), (By.NAME, "dateTo"),
            (By.CSS_SELECTOR, "input[placeholder='ДД.ММ.ГГГГ']"),
        ]
        for by, sel in find_candidates:
            try:
                el = WebDriverWait(driver, 5).until(EC.presence_of_element_located((by, sel)))
                if start_field is None:
                    start_field = el
                    continue
                if end_field is None:
                    end_field = el
                    break
            except Exception:
                continue

        if start_field is None:
            try:
                start_field = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="startDate"]')))
            except Exception:
                pass
        if end_field is None:
            try:
                end_field = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '//*[@id="endDate"]')))
            except Exception:
                pass

        if start_field is not None:
            _open_date_input(driver, start_field)
        if end_field is not None:
            _open_date_input(driver, end_field)


        date_str = business_dttm.astimezone(tz).strftime("%d.%m.%Y")
        for el in (start_field, end_field):
            if el is None:
                continue
            try:
                _set_date_input_js(driver, el, date_str)
                el.clear()
                el.send_keys(date_str)
                el.send_keys(Keys.TAB)
            except Exception:
                try:
                    _set_date_input_js(driver, el, date_str)
                except Exception:
                    pass

        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//button[.//text()[contains(., 'Save') or contains(., 'Сохранить')]]"
            ))
        ).click()

        WebDriverWait(driver, 40).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[class^='Reports-table__wrapper']"))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "div[class^='Reports-table-row__']")
        seen: set[str] = set()
        report_ids: List[str] = []

        for row in rows:
            try:
                btn = row.find_element(By.TAG_NAME, "button")
                raw = (btn.text or "").strip().replace("\u00A0", "").replace(" ", "")
                if raw.isdigit() and len(raw) >= 10 and raw not in seen:
                    seen.add(raw)
                    report_ids.append(raw)
            except Exception:
                continue

        if not report_ids:
            context.log.warning(f"[get_report_jobs] Не нашли отчёты за {date_str} для company_id={company_id}")
            return []

        cookies = _cookies_dict(driver)
        authv3 = driver.execute_script(
            "return window.localStorage.getItem('wb-eu-passport-v2.access-token');"
        )

        jobs: List[Dict[str, Any]] = []
        for rid in report_ids:
            job: Dict[str, Any] = {
                "report_id": rid,
                "report_date": business_dttm.date(),
                "cookies": cookies,
            }
            if authv3:
                job["authv3"] = authv3
                job["headers"] = {
                    "x-supplier-auth-v3": authv3,
                    "authorizev3": authv3,
                }
            jobs.append(job)

        context.log.info(f"[get_report_jobs] {len(jobs)} report(s) for {date_str}, company_id={company_id}")
        return jobs

    finally:
        try:
            driver.quit()
        except Exception:
            pass


SALE_ZIP_API = (
    "https://seller-services.wildberries.ru/ns/reports/"
    "seller-wb-balance/api/v1/reports/{report_id}/details/archived-excel"
)

async def download_one(context, job: dict) -> dict:
    rid     = job["report_id"]
    cookies = job.get("cookies") or {}
    authv3  = job.get("authv3") or (job.get("headers") or {}).get("x-supplier-auth-v3", "")

    headers = {
        "accept": "*/*",
        "authorizev3": authv3,
        "origin": "https://seller.wildberries.ru",
        "referer": "https://seller.wildberries.ru/",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138 Safari/537.36",
    }
    xsid = cookies.get("x-supplier-id") or cookies.get("x-supplier-id-external")
    if xsid:
        headers["X-Supplier-Id"] = xsid

    url = SALE_ZIP_API.format(report_id=rid)

    import time, requests
    t0 = time.time()
    r = requests.get(url, headers=headers, cookies=cookies, timeout=120)
    resp_dttm = _now_tz(context)

    try:
        body_parsed = r.json()
        body_text = json.dumps(body_parsed, ensure_ascii=False)
    except Exception:
        body_text = r.text

    return {
        "status": r.status_code,
        "headers": dict(r.headers),
        "response_dttm": resp_dttm,
        "request_parameters": {
            "reportId": rid,
            "reportDate": str(job.get("report_date") or ""),
            "elapsedMs": int((time.time() - t0) * 1000),
        },
        "response_body": body_text,
        "payload": body_text,  # фабрика тоже читает payload
    }