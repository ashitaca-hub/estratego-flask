import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "12"))

def rpc(fn: str, payload: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_KEY")
    url = f"{SUPABASE_URL}/rest/v1/rpc/{fn}"
    r = requests.post(url, headers=HEADERS_SB, json=payload, timeout=HTTP_TIMEOUT)
    if r.status_code >= 300:
        raise RuntimeError(f"RPC {fn} failed: {r.status_code} {r.text}")
    return r.json()

def get_matchup_hist_vector(p_id: str, o_id: str, yrs: int, tname: str, month: int):
    data = rpc("get_matchup_hist_vector", {
        "p_id": p_id, "o_id": o_id, "yrs": yrs, "tname": tname, "month": month
    })
    return (data[0] if isinstance(data, list) and data else {}) or {}

def get_tourney_meta(tname: str):
    data = rpc("get_tourney_meta", {"tname": tname})
    return (data[0] if isinstance(data, list) and data else {}) or {}
