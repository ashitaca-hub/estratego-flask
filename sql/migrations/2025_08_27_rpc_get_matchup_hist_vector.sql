-- Requisitos: norm_tourney(), fs_matches_long, tourney_speed_resolved
-- Crea una RPC que devuelve JSONB con deltas HIST suavizados (k por defecto=8)
-- y la meta del torneo (surface/speed_bucket).
-- Parámetros:
--   p_player_id INT      (id canónico interno)
--   p_opponent_id INT
--   p_years_back INT     (por defecto 4)
--   p_as_of DATE         (por defecto current_date)
--   p_tournament_name TEXT (para resolver surface/speed)
--   p_month INT          (si no se pasa, usa EXTRACT(MONTH FROM p_as_of))
--   p_k_month INT, p_k_surface INT, p_k_speed INT  (suavizado)
--
CREATE OR REPLACE FUNCTION public.get_matchup_hist_vector(
  p_player_id     int,
  p_opponent_id   int,
  p_years_back    int   DEFAULT 4,
  p_as_of         date  DEFAULT current_date,
  p_tournament_name text DEFAULT NULL,
  p_month         int   DEFAULT NULL,
  p_k_month       int   DEFAULT 8,
  p_k_surface     int   DEFAULT 8,
  p_k_speed       int   DEFAULT 8
)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_mon int;
  v_surf text;
  v_sb   text;
  -- conteos player
  pm_played int; pm_wins int;
  ps_played int; ps_wins int;
  pv_played int; pv_wins int;
  -- conteos opponent
  om_played int; om_wins int;
  os_played int; os_wins int;
  ov_played int; ov_wins int;
  -- winrates suavizadas
  wr_m_p float; wr_m_o float;
  wr_s_p float; wr_s_o float;
  wr_v_p float; wr_v_o float;
  -- deltas
  d_m float; d_s float; d_v float;
BEGIN
  -- Mes
  v_mon := COALESCE(p_month, EXTRACT(MONTH FROM p_as_of)::int);

  -- Meta de torneo (resolver) + fallback por superficie
  SELECT r.surface, r.speed_bucket
  INTO v_surf, v_sb
  FROM public.tourney_speed_resolved r
  WHERE public.norm_tourney(p_tournament_name) = r.tourney_key
  LIMIT 1;

  IF v_surf IS NULL THEN
    v_surf := 'hard';
  END IF;

  IF v_sb IS NULL THEN
    v_sb := CASE
      WHEN lower(v_surf) = 'grass' THEN 'Fast'
      WHEN lower(v_surf) IN ('indoor hard','indoor') THEN 'Fast'
      WHEN lower(v_surf) = 'clay'  THEN 'Slow'
      WHEN lower(v_surf) = 'hard'  THEN 'Medium'
      ELSE NULL
    END;
  END IF;

  -- ===========================
  -- Conteos para PLAYER
  -- ===========================
  SELECT
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f.match_date) = v_mon),
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f.match_date) = v_mon AND f.winner_id = p_player_id),

    COUNT(*) FILTER (
      WHERE lower(COALESCE(f.surface, c.surface)) = lower(v_surf)
    ),
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f.surface, c.surface)) = lower(v_surf)
        AND f.winner_id = p_player_id
    ),

    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c.speed_bucket,
          CASE
            WHEN c.speed_rank IS NULL THEN NULL
            WHEN c.speed_rank <= 33 THEN 'Slow'
            WHEN c.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
          END
        )
      ) = lower(v_sb)
    ),
    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c.speed_bucket,
          CASE
            WHEN c.speed_rank IS NULL THEN NULL
            WHEN c.speed_rank <= 33 THEN 'Slow'
            WHEN c.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
          END
        )
      ) = lower(v_sb)
      AND f.winner_id = p_player_id
    )
  INTO pm_played, pm_wins, ps_played, ps_wins, pv_played, pv_wins
  FROM public.fs_matches_long f
  LEFT JOIN public.tourney_speed_resolved c
    ON public.norm_tourney(f.tournament_name) = c.tourney_key
  WHERE f.player_id = p_player_id
    AND f.match_date >= (p_as_of - make_interval(years => p_years_back))
    AND f.match_date <  p_as_of;

  -- ===========================
  -- Conteos para OPPONENT
  -- ===========================
  SELECT
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f.match_date) = v_mon),
    COUNT(*) FILTER (WHERE EXTRACT(MONTH FROM f.match_date) = v_mon AND f.winner_id = p_opponent_id),

    COUNT(*) FILTER (
      WHERE lower(COALESCE(f.surface, c.surface)) = lower(v_surf)
    ),
    COUNT(*) FILTER (
      WHERE lower(COALESCE(f.surface, c.surface)) = lower(v_surf)
        AND f.winner_id = p_opponent_id
    ),

    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c.speed_bucket,
          CASE
            WHEN c.speed_rank IS NULL THEN NULL
            WHEN c.speed_rank <= 33 THEN 'Slow'
            WHEN c.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
          END
        )
      ) = lower(v_sb)
    ),
    COUNT(*) FILTER (
      WHERE lower(
        COALESCE(
          c.speed_bucket,
          CASE
            WHEN c.speed_rank IS NULL THEN NULL
            WHEN c.speed_rank <= 33 THEN 'Slow'
            WHEN c.speed_rank <= 66 THEN 'Medium'
            ELSE 'Fast'
          END
        )
      ) = lower(v_sb)
      AND f.winner_id = p_opponent_id
    )
  INTO om_played, om_wins, os_played, os_wins, ov_played, ov_wins
  FROM public.fs_matches_long f
  LEFT JOIN public.tourney_speed_resolved c
    ON public.norm_tourney(f.tournament_name) = c.tourney_key
  WHERE f.player_id = p_opponent_id
    AND f.match_date >= (p_as_of - make_interval(years => p_years_back))
    AND f.match_date <  p_as_of;

  -- Suavizado Beta/Laplace: (wins + 0.5*k)/(played + k)
  wr_m_p := (COALESCE(pm_wins,0) + 0.5*p_k_month  )::float / NULLIF(COALESCE(pm_played,0) + p_k_month, 0);
  wr_m_o := (COALESCE(om_wins,0) + 0.5*p_k_month  )::float / NULLIF(COALESCE(om_played,0) + p_k_month, 0);
  wr_s_p := (COALESCE(ps_wins,0) + 0.5*p_k_surface)::float / NULLIF(COALESCE(ps_played,0) + p_k_surface, 0);
  wr_s_o := (COALESCE(os_wins,0) + 0.5*p_k_surface)::float / NULLIF(COALESCE(os_played,0) + p_k_surface, 0);
  wr_v_p := (COALESCE(pv_wins,0) + 0.5*p_k_speed  )::float / NULLIF(COALESCE(pv_played,0) + p_k_speed, 0);
  wr_v_o := (COALESCE(ov_wins,0) + 0.5*p_k_speed  )::float / NULLIF(COALESCE(ov_played,0) + p_k_speed, 0);

  -- Manejo de 0/0: si divisor nulo -> 0.5 (neutral)
  wr_m_p := COALESCE(wr_m_p, 0.5);  wr_m_o := COALESCE(wr_m_o, 0.5);
  wr_s_p := COALESCE(wr_s_p, 0.5);  wr_s_o := COALESCE(wr_s_o, 0.5);
  wr_v_p := COALESCE(wr_v_p, 0.5);  wr_v_o := COALESCE(wr_v_o, 0.5);

  -- Deltas
  d_m := wr_m_p - wr_m_o;
  d_s := wr_s_p - wr_s_o;
  d_v := wr_v_p - wr_v_o;

  RETURN jsonb_build_object(
    'surface', v_surf,
    'speed_bucket', v_sb,

    -- Conteos (debug/coverage)
    'played_m_p', pm_played, 'wins_m_p', pm_wins,
    'played_m_o', om_played, 'wins_m_o', om_wins,
    'played_surf_p', ps_played, 'wins_surf_p', ps_wins,
    'played_surf_o', os_played, 'wins_surf_o', os_wins,
    'played_spd_p', pv_played, 'wins_spd_p', pv_wins,
    'played_spd_o', ov_played, 'wins_spd_o', ov_wins,

    -- Winrates suavizadas
    'wr_month_p', wr_m_p, 'wr_month_o', wr_m_o,
    'wr_surf_p',  wr_s_p, 'wr_surf_o',  wr_s_o,
    'wr_speed_p', wr_v_p, 'wr_speed_o', wr_v_o,

    -- Deltas HIST
    'd_hist_month',   d_m,
    'd_hist_surface', d_s,
    'd_hist_speed',   d_v
  );
END
$$;

-- Permisos RPC
GRANT EXECUTE ON FUNCTION public.get_matchup_hist_vector(
  int,int,int,date,text,int,int,int,int
) TO anon, authenticated, service_role;
