# -*- coding: utf-8 -*-
import os, math
import psycopg2
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score

def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "")
    try:
        return int(v) if str(v).strip() != "" else default
    except ValueError:
        return default

def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "")
    try:
        return float(v) if str(v).strip() != "" else default
    except ValueError:
        return default

DATABASE_URL = os.environ["DATABASE_URL"]
YEARS_BACK   = env_int("BT_YEARS", 3)        # ventana histórica as-of
MAX_ROWS     = env_int("BT_MAX", 1500)       # limita filas para CI
W_MONTH      = env_float("W_MONTH", 1.0)
W_SURF       = env_float("W_SURF",  1.0)
W_SPEED      = env_float("W_SPEED", 1.0)

SQL = f"""
WITH uniq AS (
  -- One canonical row per match (last 3 years)
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
  SELECT
    row_number() OVER () AS rid,
    u.*,
    EXTRACT(MONTH FROM u.match_date)::int AS mon,
    -- Resolver de velocidad + fallback por superficie (sube cobertura)
    COALESCE(
      c.speed_bucket,
      CASE
        WHEN lower(u.surface) = 'grass' THEN 'Fast'
        WHEN lower(u.surface) IN ('indoor hard','indoor') THEN 'Fast'
        WHEN lower(u.surface) = 'clay' THEN 'Slow'
        WHEN lower(u.surface) = 'hard' THEN 'Medium'
        ELSE NULL
      END
    ) AS sb_meta
  FROM uniq u
  LEFT JOIN public.tourney_speed_resolved c
    ON public.norm_tourney(u.tournament_name) = c.tourney_key
),
part AS (
  -- Explode each match into two participants (player/opponent)
  SELECT rid, match_date, mon, surface AS meta_surf, sb_meta, p_high AS pid, winner_id, 1 AS is_player FROM meta
  UNION ALL
  SELECT rid, match_date, mon, surface AS meta_surf, sb_meta, p_low  AS pid, winner_id, 0 AS is_player FROM meta
),
hist AS (
  -- As-of history for each participant in a single pass
  SELECT
    p.rid,
    p.is_player,

    -- Month winrate
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon) AS played_m,
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon AND f2.winner_id = p.pid) AS wins_m,

    -- Surface winrate (coalesce f2.surface con la que aporte el resolver)
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
    ) AS played_surf,
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
        AND f2.winner_id = p.pid
    ) AS wins_surf,

    -- Speed bucket winrate: compara bucket del historial con sb_meta (resolver) con fallback por superficie
    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c2.speed_bucket,
          CASE
            WHEN lower(COALESCE(f2.surface,'')) = 'grass' THEN 'Fast'
            WHEN lower(COALESCE(f2.surface,'')) IN ('indoor hard','indoor') THEN 'Fast'
            WHEN lower(COALESCE(f2.surface,'')) = 'clay'  THEN 'Slow'
            WHEN lower(COALESCE(f2.surface,'')) = 'hard'  THEN 'Medium'
            ELSE NULL
          END
        )
      ) = lower(p.sb_meta)
    ) AS played_spd,
    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c2.speed_bucket,
          CASE
            WHEN lower(COALESCE(f2.surface,'')) = 'grass' THEN 'Fast'
            WHEN lower(COALESCE(f2.surface,'')) IN ('indoor hard','indoor') THEN 'Fast'
            WHEN lower(COALESCE(f2.surface,'')) = 'clay'  THEN 'Slow'
            WHEN lower(COALESCE(f2.surface,'')) = 'hard'  THEN 'Medium'
            ELSE NULL
          END
        )
      ) = lower(p.sb_meta)
      AND f2.winner_id = p.pid
    ) AS wins_spd

  FROM part p
  LEFT JOIN public.fs_matches_long f2
    ON f2.player_id = p.pid
   AND f2.match_date >= (p.match_date - make_interval(years => {YEARS_BACK}))
   AND f2.match_date <  p.match_date
  LEFT JOIN public.tourney_speed_resolved c2
    ON public.norm_tourney(f2.tournament_name) = c2.tourney_key
  GROUP BY p.rid, p.is_player
),
pivot AS (
  SELECT
    rid,
    MAX(CASE WHEN is_player=1 THEN wins_m::float/NULLIF(played_m,0) END) AS wr_month_p,
    MAX(CASE WHEN is_player=0 THEN wins_m::float/NULLIF(played_m,0) END) AS wr_month_o,

    MAX(CASE WHEN is_player=1 THEN wins_surf::float/NULLIF(played_surf,0) END) AS wr_surf_p,
    MAX(CASE WHEN is_player=0 THEN wins_surf::float/NULLIF(played_surf,0) END) AS wr_surf_o,

    MAX(CASE WHEN is_player=1 THEN wins_spd::float/NULLIF(played_spd,0) END) AS wr_speed_p,
    MAX(CASE WHEN is_player=0 THEN wins_spd::float/NULLIF(played_spd,0) END) AS wr_speed_o
  FROM hist
  GROUP BY rid
)
SELECT
  m.match_date, m.tournament_name, m.surface, m.mon,
  m.p_high AS player_id, m.p_low AS opponent_id,
  m.winner_id,
  p.wr_month_p, p.wr_month_o,
  p.wr_surf_p,  p.wr_surf_o,
  p.wr_speed_p, p.wr_speed_o
FROM meta m
JOIN pivot p USING (rid)
ORDER BY m.match_date DESC;
"""

def sigmoid(x: float) -> float:
    try:
        return 1.0/(1.0+math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def main():
    # Connect and relax statement_timeout a bit for this session
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s';")
    conn.autocommit = False

    df = pd.read_sql(SQL, conn)
    conn.close()

    # deltas (NaN -> 0.5 => delta 0)
    for a,b,new in [
        ("wr_month_p","wr_month_o","d_month"),
        ("wr_surf_p","wr_surf_o","d_surface"),
        ("wr_speed_p","wr_speed_o","d_speed"),
    ]:
        df[new] = (df[a].fillna(0.5) - df[b].fillna(0.5))

    # etiqueta: gana player_id
    df["y"] = (df["winner_id"] == df["player_id"]).astype(int)

    # combinación logística de deltas (pesos)
    denom = max(1.0, (abs(W_MONTH)+abs(W_SURF)+abs(W_SPEED)))
    z = (W_MONTH*df["d_month"] + W_SURF*df["d_surface"] + W_SPEED*df["d_speed"]) / denom
    df["p_hat"] = z.apply(sigmoid)

    # métricas
    try:
        ll = log_loss(df["y"], df["p_hat"], labels=[0,1])
    except ValueError:
        ll = float('nan')
    try:
        auc = roc_auc_score(df["y"], df["p_hat"])
    except ValueError:
        auc = float('nan')
    acc = (df["p_hat"]>=0.5).astype(int).mean()

    # Coberturas por dimensión (ambos lados con valor)
    cov_month   = float(df[["wr_month_p","wr_month_o"]].notna().all(axis=1).mean())
    cov_surface = float(df[["wr_surf_p","wr_surf_o"]].notna().all(axis=1).mean())
    cov_speed   = float(df[["wr_speed_p","wr_speed_o"]].notna().all(axis=1).mean())

    print("== SUMMARY ==")
    print(f"rows: {len(df)}")
    print(f"log_loss: {ll}")
    print(f"auc: {auc}")
    print(f"accuracy@0.5: {acc}")
    print(f"avg_p: {float(df['p_hat'].mean())}")
    full_rows = df[["wr_month_p","wr_month_o","wr_surf_p","wr_surf_o","wr_speed_p","wr_speed_o"]].notna().all(axis=1).mean()
    null_rate = 1.0 - float(full_rows)
    print(f"null_rate: {null_rate}")
    print(f"coverage_month: {cov_month}")
    print(f"coverage_surface: {cov_surface}")
    print(f"coverage_speed: {cov_speed}")

    # Save CSV in repo workspace
    out_csv = "backtest_hist_asof.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

if __name__ == "__main__":
    main()
