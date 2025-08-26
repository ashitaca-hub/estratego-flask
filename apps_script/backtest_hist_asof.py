import os, math
import psycopg2
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score

DATABASE_URL = os.environ["DATABASE_URL"]
YEARS_BACK = int(os.environ.get("BT_YEARS", "4"))
MAX_ROWS   = int(os.environ.get("BT_MAX", "1500"))  # limita para CI

SQL = f"""
WITH uniq AS (
  -- una fila por partido (canónica), últimos 3 años para no eternizar
  SELECT
    f.match_date,
    f.tournament_name,
    f.surface,
    LEAST(f.player_id, f.opponent_id) AS p_low,
    GREATEST(f.player_id, f.opponent_id) AS p_high,
    MAX(f.winner_id) AS winner_id
  FROM public.fs_matches_long f
  WHERE f.match_date BETWEEN (current_date - interval '3 years') AND current_date
  GROUP BY 1,2,3,4,5
  ORDER BY match_date DESC
  LIMIT {MAX_ROWS}
),
meta AS (
  SELECT u.*,
         EXTRACT(MONTH FROM u.match_date)::int AS mon,
         c.speed_bucket
  FROM uniq u
  LEFT JOIN public.court_speed_rankig_norm_compat c
    ON lower(u.tournament_name) = lower(c.tournament_name)
)
SELECT
  match_date, tournament_name, surface, mon,
  p_high AS player_id, p_low AS opponent_id,
  winner_id,
  -- HIST as-of para ambos
  public.fs_month_winrate_asof(p_high, mon, {YEARS_BACK}, match_date)  AS wr_month_p,
  public.fs_month_winrate_asof(p_low,  mon, {YEARS_BACK}, match_date)  AS wr_month_o,
  public.fs_surface_winrate_asof(p_high, surface, {YEARS_BACK}, match_date) AS wr_surf_p,
  public.fs_surface_winrate_asof(p_low,  surface, {YEARS_BACK}, match_date) AS wr_surf_o,
  public.fs_speed_winrate_asof(p_high, speed_bucket, {YEARS_BACK}, match_date) AS wr_speed_p,
  public.fs_speed_winrate_asof(p_low,  speed_bucket, {YEARS_BACK}, match_date) AS wr_speed_o
FROM meta
"""

def sigmoid(x: float) -> float:
    try:
        return 1.0/(1.0+math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def main():
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql(SQL, conn)
    conn.close()

    # deltas (si falta algún wr_x, tratamos como 0.5 -> delta 0)
    for a,b,new in [
        ("wr_month_p","wr_month_o","d_month"),
        ("wr_surf_p","wr_surf_o","d_surface"),
        ("wr_speed_p","wr_speed_o","d_speed"),
    ]:
        df[new] = (df[a].fillna(0.5) - df[b].fillna(0.5))

    # etiqueta: 1 si gana "player_id" (p_high), 0 si no
    df["y"] = (df["winner_id"] == df["player_id"]).astype(int)

    # score lineal + logística (pesos iniciales iguales)
    w_month = float(os.environ.get("W_MONTH", "1.0"))
    w_surf  = float(os.environ.get("W_SURF",  "1.0"))
    w_speed = float(os.environ.get("W_SPEED", "1.0"))

    z = (w_month*df["d_month"] + w_surf*df["d_surface"] + w_speed*df["d_speed"]) / max(1.0, (abs(w_month)+abs(w_surf)+abs(w_speed)))
    df["p_hat"] = z.apply(sigmoid)  # convierte a [0,1]

    # métricas
    try:
        ll = log_loss(df["y"], df["p_hat"], labels=[0,1])
    except ValueError:
        ll = float('nan')
    try:
        auc = roc_auc_score(df["y"], df["p_hat"])
    except ValueError:
        auc = float('nan')
    acc = accuracy_score(df["y"], (df["p_hat"]>=0.5).astype(int))

    summary = {
        "rows": len(df),
        "log_loss": ll,
        "auc": auc,
        "accuracy@0.5": acc,
        "avg_p": float(df["p_hat"].mean()),
        "null_rate": float(1.0 - df[["wr_month_p","wr_month_o","wr_surf_p","wr_surf_o","wr_speed_p","wr_speed_o"]].notna().all(axis=1).mean()),
    }
    print("== SUMMARY ==")
    for k,v in summary.items():
        print(f"{k}: {v}")

    out_csv = "/mnt/data/backtest_hist_asof.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

if __name__ == "__main__":
    main()
