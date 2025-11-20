import os
from dotenv import load_dotenv
load_dotenv()
import psycopg2
import psycopg2.extras

# —————————————————————————————————————————————————————————
# 1) Загрузка .env
# —————————————————————————————————————————————————————————
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    raise RuntimeError("Не заданы все переменные DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME")

DSN = (
    f"host={DB_HOST} "
    f"port={DB_PORT} "
    f"dbname={DB_NAME} "
    f"user={DB_USER} "
    f"password={DB_PASSWORD}"
)

DEFAULT_SELLER = os.getenv("DEFAULT_SELLER", "main_seller")


# —————————————————————————————————————————————————————————
# 2) Сбор «сырых» данных из БД
# —————————————————————————————————————————————————————————
def _load_raw_config(seller_code: str):
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # 2.1) ID seller
    cur.execute("SELECT id FROM sellers WHERE code = %s", (seller_code,))
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Seller {seller_code!r} not found")
    seller_id = row["id"]

    raw = {}

    # 2.2) seller-level config_items
    cur.execute("""
      SELECT provider, cfg
        FROM config_items
       WHERE owner_type='seller'
         AND owner_id  =%s
    """, (seller_id,))
    for provider, blob in cur.fetchall():
        raw[provider] = blob  # blob уже dict или list

    # 2.3) seller-level секреты
    cur.execute("""
      SELECT key, value
        FROM config_secrets
       WHERE owner_type='seller'
         AND owner_id   =%s
    """, (seller_id,))
    for key, val in cur.fetchall():
        if key == "imap_password":
            raw.setdefault("imap", {})["password"] = val
        elif key == "postgres_password":
            raw.setdefault("data_storage", {}).setdefault("postgres", {})["password"] = val
        elif key.startswith("betapro_partner_"):
            pid = key.split("_")[2]
            for item in raw.get("betapro_partners", []):
                if item.get("partner_id") == pid:
                    item["password"] = val

    # 2.4) legal_entities + их конфиги
    raw["entities"] = {}
    cur.execute("""
      SELECT id, code, display_name
        FROM legal_entities
       WHERE seller_id=%s
    """, (seller_id,))
    for ent_id, code, display_name in cur.fetchall():
        # сразу сохраняем только тройку: id, code, display_name
        ent = {
            "id":           ent_id,
            "code":         code,
            "display_name": display_name
        }

        # entity-level config_items
        cur.execute("""
          SELECT provider, cfg
            FROM config_items
           WHERE owner_type='entity'
             AND owner_id   =%s
        """, (ent_id,))
        for prov, blob in cur.fetchall():
            ent[prov] = blob

        # entity-level секреты
        cur.execute("""
          SELECT key, value
            FROM config_secrets
           WHERE owner_type='entity'
             AND owner_id   =%s
        """, (ent_id,))
        for key, val in cur.fetchall():
            ent[key] = val

        raw["entities"][code] = ent

    cur.close()
    conn.close()
    return raw


# —————————————————————————————————————————————————————————
# 3) Преобразование «сырых» данных в старый формат JSON
# —————————————————————————————————————————————————————————
def _normalize_config(raw: dict) -> dict:
    out = {}

    # — прокидываем неизменённые блоки целиком
    for key in ("imap", "google_sheets", "data_storage", "marketplace_api", "report_paths"):
        if key in raw:
            out[key] = raw[key]

    # 1) betapro_partners
    out["betapro_partners"] = raw.get("betapro_partners", [])

    # 2) wb_marketplace_keys — старый API-токен для price/discounts
    out["wb_marketplace_keys"] = {
        code: ent["wb_api_token"]
        for code, ent in raw.get("entities", {}).items()
        if "wb_api_token" in ent
    }

    # 3) wb_autobidder_keys — новый рекламный токен
    out["wb_autobidder_keys"] = {
        code: ent["wb_autobidder_token"]
        for code, ent in raw.get("entities", {}).items()
        if "wb_autobidder_token" in ent
    }

    # 4) ozon_partners
    out["ozon_partners"] = {
        code: {
            "api_key":   ent["ozon_api_token"],
            "client_id": ent["ozon_api_client_id"]
        }
        for code, ent in raw.get("entities", {}).items()
        if "ozon_api_token" in ent and "ozon_api_client_id" in ent
    }

    # 5) ozon_ad_campaign
    out["ozon_ad_campaign"] = {
        code: {
            "client_id":     ent["ozon_ad_client_id"],
            "client_secret": ent["ozon_ad_client_token"]
        }
        for code, ent in raw.get("entities", {}).items()
        if "ozon_ad_client_id" in ent and "ozon_ad_client_token" in ent
    }

    # 6) wildberries_selenium (merge seller- и entity-level)
    wb = raw.get("wildberries_selenium", {})
    seller_cp = wb.get("chrome_profiles", {})
    ent_cp = {
        code: ent["selenium_profile"]["wildberries"]
        for code, ent in raw.get("entities", {}).items()
        if ent.get("selenium_profile", {}).get("wildberries")
    }
    out["wildberries_selenium"] = {
        "chrome_profiles": {**seller_cp, **ent_cp},
        "sale_report": {
            "url":   wb.get("report_url"),
            "paths": wb.get("report_paths", {})
        },
        **({"localization_index": wb["localization_index"]}
            if wb.get("localization_index") else {})
    }

    # 7) ozon_selenium
    oz = raw.get("ozon_selenium", {})
    seller_cp = oz.get("chrome_profiles", {})
    ent_cp = {
        code: ent["selenium_profile"]["ozon"]
        for code, ent in raw.get("entities", {}).items()
        if ent.get("selenium_profile", {}).get("ozon")
    }
    out["ozon_selenium"] = {"chrome_profiles": {**seller_cp, **ent_cp}}

    # 8) mpm_selenium
    mpm = raw.get("mpm_selenium", {})
    seller_cp = mpm.get("chrome_profiles", {})
    ent_cp = {
        code: ent["selenium_profile"]["mpm"]
        for code, ent in raw.get("entities", {}).items()
        if ent.get("selenium_profile", {}).get("mpm")
    }
    out["mpm_selenium"] = {"chrome_profiles": {**seller_cp, **ent_cp}}

    return out


# —————————————————————————————————————————————————————————
# 4) Основные функции — публичный API
# —————————————————————————————————————————————————————————
def load_config():
    raw = _load_raw_config(DEFAULT_SELLER)
    normalized = _normalize_config(raw)

    # Собираем чистые метаданные сущностей — только id, code и display_name
    entities_meta = {
        code: {
            "id":           ent["id"],
            "code":         ent["code"],
            "display_name": ent["display_name"],
        }
        for code, ent in raw["entities"].items()
    }
    normalized["entities_meta"] = entities_meta

    return normalized

def load_new_config():
    raw = _load_raw_config('new_seller')
    normalized = _normalize_config(raw)

    # Собираем чистые метаданные сущностей — только id, code и display_name
    entities_meta = {
        code: {
            "id":           ent["id"],
            "code":         ent["code"],
            "display_name": ent["display_name"],
        }
        for code, ent in raw["entities"].items()
    }
    normalized["entities_meta"] = entities_meta

    return normalized

def load_avangard_seller():
    raw = _load_raw_config('avangard_seller')
    normalized = _normalize_config(raw)

    # Собираем чистые метаданные сущностей — только id, code и display_name
    entities_meta = {
        code: {
            "id":           ent["id"],
            "code":         ent["code"],
            "display_name": ent["display_name"],
        }
        for code, ent in raw["entities"].items()
    }
    normalized["entities_meta"] = entities_meta

    return normalized


def load_all_configs() -> dict[str, dict]:
    """
    Вернёт мэппинг seller_code -> его нормализованный конфиг.
    """
    conn = psycopg2.connect(DSN)
    cur  = conn.cursor()
    cur.execute("SELECT code FROM sellers")
    codes = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    result = {}
    for code in codes:
        # для каждого селлера запускаем ту же логику
        raw = _load_raw_config(code)
        normalized = _normalize_config(raw)
        entities_meta = {
            c: {"id": e["id"], "code": e["code"], "display_name": e["display_name"]}
            for c, e in raw["entities"].items()
        }
        normalized["entities_meta"] = entities_meta
        result[code] = normalized

    return result

def load_db_configs():
    return {
        "source": {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        },
        "dest": {
            "host": os.getenv("DEST_DB_HOST"),
            "port": int(os.getenv("DEST_DB_PORT", 5432)),
            "dbname": os.getenv("DEST_DB_NAME"),
            "user": os.getenv("DEST_DB_USER"),
            "password": os.getenv("DEST_DB_PASSWORD"),
            "sslmode": os.getenv("DEST_DB_SSLMODE", "disable"),
        },
    }