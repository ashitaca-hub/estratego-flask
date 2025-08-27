# -*- coding: utf-8 -*-
import os, math, itertools, json
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
YEARS_BACK   = env_int("BT_YEARS", 3)         # ventana histórica
MAX_ROWS     = env_int("BT_MAX", 1500)        # límite CI

# Pesos (si GRID=0, se usan estos)
W_MONTH      = env_float("W_MONTH", 1.0)
W_SURF       = env_float("W_SURF",  1.0)
W_SPEED      = env_float("W_SPEED", 1.0)

# Suavizado bayesiano: (wins + k*0.5)/(played + k)
K_MONTH      = env_int("K_MONTH", 8)          # pseudo-partidos para mes
K_SURF       = env_int("K_SURF",  8)          # pseudo-partidos para superficie
K_SPEED      = env_int("K_SPEED", 8)          # pseudo-partidos para velocidad

# Grid search (si GRID=1 se exploran combinaciones de pesos)
GRID         = os.getenv("GRID", "0") == "1"

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
  -- As-of history (counts)
  SELECT
    p.rid,
    p.is_player,

    -- Month counts
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon) AS played_m,
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f2.match_date) = p.mon AND f2.winner_id = p.pid) AS wins_m,

    -- Surface counts (coalesce f2.surface con la del resolver)
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
    ) AS played_surf,
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f2.surface, c2.surface)) = lower(p.meta_surf)
        AND f2.winner_id = p.pid
    ) AS wins_surf,

    -- Speed counts: compara bucket historial vs sb_meta (resolver/fallback)
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
  -- Carry both sides' counts to rows
  SELECT
    rid,
    MAX(CASE WHEN is_player=1 THEN played_m END)  AS played_m_p,
    MAX(CASE WHEN is_player=1 THEN wins_m   END)  AS wins_m_p,
    MAX(CASE WHEN is_player=0 THEN played_m END)  AS played_m_o,
    MAX(CASE WHEN is_player=0 THEN wins_m   END)  AS wins_m_o,

    MAX(CASE WHEN is_player=1 THEN played_surf END) AS played_surf_p,
    MAX(CASE WHEN is_player=1 THEN wins_surf   END) AS wins_surf_p,
    MAX(CASE WHEN is_player=0 THEN played_surf END) AS played_surf_o,
    MAX(CASE WHEN is_player=0 THEN wins_surf   END) AS wins_surf_o,

    MAX(CASE WHEN is_player=1 THEN played_spd END) AS played_spd_p,
    MAX(CASE WHEN is_player=1 THEN wins_spd   END) AS wins_spd_p,
    MAX(CASE WHEN is_player=0 THEN played_spd END) AS played_spd_o,
    MAX(CASE WHEN is_player=0 THEN wins_spd   END) AS wins_spd_o
  FROM hist
  GROUP BY rid
)
SELECT
  m.match_date, m.tournament_name, m.surface, m.mon,
  m.p_high AS player_id, m.p_low AS opponent_id,
  m.winner_id,
  p.*
FROM meta m
JOIN pivot p USING (rid)
ORDER BY m.match_date DESC;
"""

def sigmoid(x: float) -> float:
    try:
        return 1.0/(1.0+math.exp(-x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0

def smooth(wins, played, k):
    # (wins + k*0.5)/(played + k)
    return (wins + 0.5*k) / (played + k) if (played is not None and played >= 0) else 0.5

def evaluate(df, w_month, w_surf, w_speed, k_month, k_surf, k_speed):
    # winrates suavizadas
    wr_m_p = df.apply(lambda r: smooth(r["wins_m_p"] or 0, r["played_m_p"] or 0, k_month), axis=1)
    wr_m_o = df.apply(lambda r: smooth(r["wins_m_o"] or 0, r["played_m_o"] or 0, k_month), axis=1)
    wr_s_p = df.apply(lambda r: smooth(r["wins_surf_p"] or 0, r["played_surf_p"] or 0, k_surf), axis=1)
    wr_s_o = df.apply(lambda r: smooth(r["wins_surf_o"] or 0, r["played_surf_o"] or 0, k_surf), axis=1)
    wr_v_p = df.apply(lambda r: smooth(r["wins_spd_p"] or 0, r["played_spd_p"] or 0, k_speed), axis=1)
    wr_v_o = df.apply(lambda r: smooth(r["wins_spd_o"] or 0, r["played_spd_o"] or 0, k_speed), axis=1)

    d_month   = wr_m_p - wr_m_o
    d_surface = wr_s_p - wr_s_o
    d_speed   = wr_v_p - wr_v_o

    denom = max(1.0, (abs(w_month)+abs(w_surf)+abs(w_speed)))
    z = (w_month*d_month + w_surf*d_surface + w_speed*d_speed) / denom
    p_hat = z.apply(sigmoid)

    y = (df["winner_id"] == df["player_id"]).astype(int)

    # métricas
    try:
        ll = log_loss(y, p_hat, labels=[0,1])
    except ValueError:
        ll = float('nan')
    try:
        auc = roc_auc_score(y, p_hat)
    except ValueError:
        auc = float('nan')
    acc = (p_hat>=0.5).astype(int).mean()

    return {
        "W_MONTH": w_month, "W_SURF": w_surf, "W_SPEED": w_speed,
        "K_MONTH": k_month, "K_SURF": k_surf, "K_SPEED": k_speed,
        "rows": int(len(df)), "log_loss": float(ll),
        "auc": float(auc), "accuracy@0.5": float(acc), "avg_p": float(p_hat.mean())
    }

def main():
    # Connect and relax statement_timeout a bit
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '120s';")
    conn.autocommit = False

    df = pd.read_sql(SQL, conn)
    conn.close()

    if GRID:
        weight_grid = [0.5, 1.0, 1.5, 2.0]
        rows = []
        for wm, ws, wv in itertools.product(weight_grid, weight_grid, weight_grid):
            res = evaluate(df, wm, ws, wv, K_MONTH, K_SURF, K_SPEED)
            rows.append(res)
        res_df = pd.DataFrame(rows).sort_values(by=["log_loss","auc"], ascending=[True, False])

        # Mejor combinación
        best = res_df.iloc[0].to_dict()
        print("== GRID BEST ==")
        print(json.dumps(best, indent=2))

        out_csv = "backtest_hist_grid.csv"
        res_df.to_csv(out_csv, index=False)
        print(f"Saved: {out_csv}")
    else:
        res = evaluate(df, W_MONTH, W_SURF, W_SPEED, K_MONTH, K_SURF, K_SPEED)
        print("== SUMMARY ==")
        for k,v in res.items():
            print(f"{k}: {v}")
        out_csv = "backtest_hist_asof.csv"
        df.to_csv(out_csv, index=False)
        print(f"Saved: {out_csv}")

if __name__ == "__main__":
    main()
