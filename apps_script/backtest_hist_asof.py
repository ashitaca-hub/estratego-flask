import os, math
import psycopg2
import pandas as pd
from sklearn.metrics import log_loss, roc_auc_score, accuracy_score

DATABASE_URL = os.environ["DATABASE_URL"]
YEARS_BACK = int(os.environ.get("BT_YEARS", "3"))
MAX_ROWS   = int(os.environ.get("BT_MAX", "800"))  # baja si tu DB es lenta (ej. 800)

SQL = f"""
WITH uniq AS (
  -- Una fila por partido (canónica) para backtest
  SELECT
    f.match_date,
    f.tournament_name,
    f.surface,
    LEAST(f.player_id, f.opponent_id)  AS p_low,
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
    row_number() over() AS rid,
    u.*,
    EXTRACT(MONTH FROM u.match_date)::int AS mon,
    c.speed_bucket AS sb_meta
  FROM uniq u
  LEFT JOIN public.court_speed_rankig_norm_compat c
    ON lower(u.tournament_name) = lower(c.tournament_name)
),
-- “Explota” cada partido en dos participantes (player/opponent)
part AS (
  SELECT rid, match_date, mon, surface AS meta_surf, sb_meta, p_high AS pid, winner_id, 1 AS is_player FROM meta
  UNION ALL
  SELECT rid, match_date, mon, surface AS meta_surf, sb_meta, p_low  AS pid, winner_id, 0 AS is_player FROM meta
),
-- Historial “as-of” con una sola pasada: unimos a todos los partidos anteriores de ese pid
hist AS (
  SELECT
    p.rid,
    p.is_player,
    -- Jugamos con la ventana [match_date - YEARS_BACK, match_date)
    COUNT(*) FILTER (
      WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon
    ) AS played_m,
    COUNT(*) FILTER (
      WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon
        AND f2.winner_id = p.pid
    ) AS wins_m,

    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
    ) AS played_surf,
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
        AND f2.winner_id = p.pid
    ) AS wins_surf,

    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c2.speed_bucket,
          CASE
            WHEN c2.speed_rank IS NULL THEN NULL
            WHEN c2.speed_rank <= 33 THEN 'Slow'
            WHEN c2.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
          END
        )
      ) = lower(p.sb_meta)
    ) AS played_spd,
    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c2.speed_bucket,
          CASE
            WHEN c2.speed_rank IS NULL THEN NULL
            WHEN c2.speed_rank <= 33 THEN 'Slow'
            WHEN c2.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
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
  LEFT JOIN public.court_speed_rankig_norm_compat c2
    ON lower(f2.tournament_name) = lower(c2.tournament_name)  -- exact/normalizado (más rápido que fuzzy)
  GROUP BY p.rid, p.is_player
),
-- Pivota a columnas P/O por rid
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
    # Conexión + aumenta statement_timeout de la sesión
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s';")  # sube si hace falta
    conn.autocommit = False

    df = pd.read_sql(SQL, conn)
    conn.close()

    # deltas (sustituye NaN por 0.5 => delta 0)
    for a,b,new in [
        ("wr_month_p","wr_month_o","d_month"),
        ("wr_surf_p","wr_surf_o","d_surface"),
        ("wr_speed_p","wr_speed_o","d_speed"),
    ]:
        df[new] = (df[a].fillna(0.5) - df[b].fillna(0.5))

    # etiqueta: gana player_id
    df["y"] = (df["winner_id"] == df["player_id"]).astype(int)

    # pesos iniciales
    w_month = float(os.environ.get("W_MONTH", "1.0"))
    w_surf  = float(os.environ.get("W_SURF",  "1.0"))
    w_speed = float(os.environ.get("W_SPEED", "1.0"))
    denom = max(1.0, (abs(w_month)+abs(w_surf)+abs(w_speed)))
    z = (w_month*df["d_month"] + w_surf*df["d_surface"] + w_speed*df["d_speed"]) / denom
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

    print("== SUMMARY ==")
    print(f"rows: {len(df)}")
    print(f"log_loss: {ll}")
    print(f"auc: {auc}")
    print(f"accuracy@0.5: {acc}")
    print(f"avg_p: {float(df['p_hat'].mean())}")
    # % filas con alguna winrate nula antes del fillna
    null_rate = 1.0 - df[["wr_month_p","wr_month_o","wr_surf_p","wr_surf_o","wr_speed_p","wr_speed_o"]].notna().all(axis=1).mean()
    print(f"null_rate: {float(null_rate)}")

    out_csv = "/mnt/data/backtest_hist_asof.csv"
    df.to_csv(out_csv, index=False)
    print(f"Saved: {out_csv}")

if __name__ == "__main__":
    main()
