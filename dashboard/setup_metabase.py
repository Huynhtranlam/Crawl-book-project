import json
import os
import time
import urllib.error
import urllib.request


METABASE_URL = os.getenv("METABASE_URL", "http://localhost:3000").rstrip("/")
SITE_NAME = os.getenv("METABASE_SITE_NAME", "BTC Market Monitoring Dashboard")
ADMIN_FIRST_NAME = os.getenv("METABASE_ADMIN_FIRST_NAME", "Dashboard")
ADMIN_LAST_NAME = os.getenv("METABASE_ADMIN_LAST_NAME", "Admin")
ADMIN_EMAIL = os.getenv("METABASE_ADMIN_EMAIL", "admin@example.com")
ADMIN_PASSWORD = os.getenv("METABASE_ADMIN_PASSWORD", "metabase123")
DATABASE_NAME = os.getenv("METABASE_DATABASE_NAME", "BTC Market Warehouse")
DATABASE_HOST = os.getenv("METABASE_DATABASE_HOST", "postgres")
DATABASE_PORT = int(os.getenv("METABASE_DATABASE_PORT", "5432"))
DATABASE_DBNAME = os.getenv("METABASE_DATABASE_DBNAME", "analytics")
DATABASE_USER = os.getenv("METABASE_DATABASE_USER", "analytics")
DATABASE_PASSWORD = os.getenv("METABASE_DATABASE_PASSWORD", "analytics123")
DATABASE_SCHEMA = os.getenv("METABASE_DATABASE_SCHEMA", "public_marts")
SETUP_TIMEOUT_SECONDS = int(os.getenv("METABASE_SETUP_TIMEOUT_SECONDS", "300"))

DASHBOARD_NAME = "BTC Market Monitoring"
DASHBOARD_DESCRIPTION = "BTCUSDT market monitoring dashboard with trend, volatility, and signal context."

CHARTS = [
    {
        "name": "BTC Latest Market Snapshot",
        "display": "table",
        "query": f"""
            select
                symbol,
                as_of_time,
                last_price,
                price_change_pct_5m,
                price_change_pct_15m,
                price_change_pct_1h,
                price_change_pct_4h,
                price_change_pct_24h,
                volume_24h,
                intraday_volatility,
                trend_regime,
                momentum_regime,
                volatility_regime
            from {DATABASE_SCHEMA}.mart_btc_price_latest
        """,
        "layout": {"row": 0, "col": 0, "size_x": 24, "size_y": 6},
    },
    {
        "name": "BTC Close Price and Moving Averages",
        "display": "line",
        "query": f"""
            select *
            from (
                select
                    candle_time,
                    close,
                    sma_20,
                    ema_20
                from {DATABASE_SCHEMA}.mart_btc_ohlcv
                order by candle_time desc
                limit 120
            ) recent_candles
            order by candle_time
        """,
        "layout": {"row": 6, "col": 0, "size_x": 24, "size_y": 8},
    },
    {
        "name": "BTC RSI 14 Trend",
        "display": "line",
        "query": f"""
            select *
            from (
                select
                    candle_time,
                    rsi_14
                from {DATABASE_SCHEMA}.mart_btc_ohlcv
                order by candle_time desc
                limit 120
            ) recent_rsi
            order by candle_time
        """,
        "layout": {"row": 14, "col": 0, "size_x": 12, "size_y": 8},
    },
    {
        "name": "BTC Volume vs Rolling Average",
        "display": "bar",
        "query": f"""
            select *
            from (
                select
                    candle_time,
                    volume,
                    volume_ma_20
                from {DATABASE_SCHEMA}.mart_btc_ohlcv
                order by candle_time desc
                limit 120
            ) recent_volume
            order by candle_time
        """,
        "layout": {"row": 14, "col": 12, "size_x": 12, "size_y": 8},
    },
    {
        "name": "BTC Multi-timeframe Returns",
        "display": "bar",
        "query": f"""
            select '5m' as timeframe, price_change_pct_5m as return_pct from {DATABASE_SCHEMA}.mart_btc_price_latest
            union all
            select '15m' as timeframe, price_change_pct_15m as return_pct from {DATABASE_SCHEMA}.mart_btc_price_latest
            union all
            select '1h' as timeframe, price_change_pct_1h as return_pct from {DATABASE_SCHEMA}.mart_btc_price_latest
            union all
            select '4h' as timeframe, price_change_pct_4h as return_pct from {DATABASE_SCHEMA}.mart_btc_price_latest
            union all
            select '24h' as timeframe, price_change_pct_24h as return_pct from {DATABASE_SCHEMA}.mart_btc_price_latest
        """,
        "layout": {"row": 22, "col": 0, "size_x": 24, "size_y": 8},
    },
]


class MetabaseError(RuntimeError):
    pass


def as_items(response):
    if isinstance(response, list):
        return response
    if isinstance(response, dict) and isinstance(response.get("data"), list):
        return response["data"]
    raise MetabaseError(f"Unexpected list response: {response}")


def request_json(method, path, payload=None, session_id=None):
    url = f"{METABASE_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if session_id:
        headers["X-Metabase-Session"] = session_id

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(request) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise MetabaseError(f"{method} {path} failed with {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise MetabaseError(f"{method} {path} failed: {exc}") from exc

    if not body:
        return None

    return json.loads(body)


def wait_for_metabase():
    deadline = time.time() + SETUP_TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            health = request_json("GET", "/api/health")
            if health.get("status") == "ok":
                return
        except MetabaseError:
            pass
        time.sleep(5)

    raise MetabaseError("Timed out waiting for Metabase to become healthy.")


def login():
    try:
        response = request_json(
            "POST",
            "/api/session",
            payload={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
        )
    except MetabaseError as exc:
        if "401" in str(exc):
            return None
        raise
    return response["id"]


def ensure_admin_setup():
    session_id = login()
    if session_id:
        return session_id

    properties = request_json("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")
    if not setup_token:
        raise MetabaseError(
            "Metabase is already initialized but the configured admin credentials are invalid. "
            "TODO: align METABASE_ADMIN_EMAIL and METABASE_ADMIN_PASSWORD with the existing instance."
        )

    request_json(
        "POST",
        "/api/setup",
        payload={
            "token": setup_token,
            "user": {
                "first_name": ADMIN_FIRST_NAME,
                "last_name": ADMIN_LAST_NAME,
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
            "prefs": {"site_name": SITE_NAME, "allow_tracking": False},
        },
    )

    session_id = login()
    if not session_id:
        raise MetabaseError("Metabase setup completed, but login still failed.")
    return session_id


def ensure_database(session_id):
    databases = as_items(request_json("GET", "/api/database", session_id=session_id))
    for database in databases:
        if database.get("name") == DATABASE_NAME:
            return database["id"]

    database = request_json(
        "POST",
        "/api/database",
        payload={
            "engine": "postgres",
            "name": DATABASE_NAME,
            "details": {
                "host": DATABASE_HOST,
                "port": DATABASE_PORT,
                "dbname": DATABASE_DBNAME,
                "user": DATABASE_USER,
                "password": DATABASE_PASSWORD,
                "schema-filters-type": "inclusion",
                "schema-filters-patterns": DATABASE_SCHEMA,
                "ssl": False,
            },
            "is_full_sync": True,
            "is_on_demand": False,
            "auto_run_queries": True,
        },
        session_id=session_id,
    )
    return database["id"]


def find_dashboard(session_id):
    dashboards = request_json("GET", "/api/dashboard", session_id=session_id)
    for dashboard in dashboards:
        if dashboard.get("name") == DASHBOARD_NAME:
            return dashboard["id"]
    return None


def create_dashboard(session_id):
    dashboard = request_json(
        "POST",
        "/api/dashboard",
        payload={"name": DASHBOARD_NAME, "description": DASHBOARD_DESCRIPTION},
        session_id=session_id,
    )
    return dashboard["id"]


def create_card(session_id, database_id, chart):
    card = request_json(
        "POST",
        "/api/card",
        payload={
            "name": chart["name"],
            "display": chart["display"],
            "visualization_settings": {},
            "dataset_query": {
                "type": "native",
                "native": {"query": chart["query"]},
                "database": database_id,
            },
        },
        session_id=session_id,
    )
    return card["id"]


def ensure_dashboard_cards(session_id, database_id, dashboard_id):
    dashboard = request_json("GET", f"/api/dashboard/{dashboard_id}", session_id=session_id)
    existing_dashcards = {
        dashcard["card"]["name"]: dashcard
        for dashcard in dashboard.get("dashcards", [])
        if dashcard.get("card")
    }

    dashcards_payload = []
    next_temporary_id = -1
    card_ids = []

    for chart in CHARTS:
        existing_dashcard = existing_dashcards.get(chart["name"])
        if existing_dashcard:
            dashcards_payload.append(
                {
                    "id": existing_dashcard["id"],
                    "card_id": existing_dashcard["card_id"],
                    "parameter_mappings": existing_dashcard.get("parameter_mappings", []),
                    "series": existing_dashcard.get("series", []),
                    **chart["layout"],
                }
            )
            card_ids.append(existing_dashcard["card_id"])
            continue

        card_id = create_card(session_id, database_id, chart)
        dashcards_payload.append(
            {
                "id": next_temporary_id,
                "card_id": card_id,
                "parameter_mappings": [],
                "series": [],
                **chart["layout"],
            }
        )
        card_ids.append(card_id)
        next_temporary_id -= 1

    request_json(
        "PUT",
        f"/api/dashboard/{dashboard_id}",
        payload={
            "id": dashboard_id,
            "name": DASHBOARD_NAME,
            "description": DASHBOARD_DESCRIPTION,
            "width": dashboard.get("width", "fixed"),
            "collection_id": dashboard.get("collection_id"),
            "parameters": dashboard.get("parameters", []),
            "tabs": dashboard.get("tabs", []),
            "dashcards": dashcards_payload,
        },
        session_id=session_id,
    )

    return card_ids


def verify_cards(session_id, card_ids):
    for card_id in card_ids:
        results = request_json(
            "POST",
            f"/api/card/{card_id}/query/json",
            payload={},
            session_id=session_id,
        )
        if not isinstance(results, list) or not results:
            raise MetabaseError(f"Card {card_id} returned no chart data.")


def main():
    wait_for_metabase()
    session_id = ensure_admin_setup()
    database_id = ensure_database(session_id)

    dashboard_id = find_dashboard(session_id)
    if dashboard_id is None:
        dashboard_id = create_dashboard(session_id)

    card_ids = ensure_dashboard_cards(session_id, database_id, dashboard_id)
    verify_cards(session_id, card_ids[:3])

    print(
        json.dumps(
            {
                "metabase_url": METABASE_URL,
                "dashboard_id": dashboard_id,
                "database_id": database_id,
                "chart_count": len(card_ids),
            }
        )
    )


if __name__ == "__main__":
    main()
