-- FS histórico: winrates por mes/superficie/velocidad (IDs internos INT)
-- Usa la vista de compatibilidad: court_speed_rankig_norm_compat (si no hay speed_bucket, la vista lo pone a NULL y se deriva por speed_rank)

-- Limpieza opcional para redeploy seguro
DROP FUNCTION IF EXISTS public.fs_month_winrate(int,int,int);
DROP FUNCTION IF EXISTS public.fs_surface_winrate(int,text,int);
DROP FUNCTION IF EXISTS public.fs_speed_winrate(int,text,int);

-- --------------------------------------------------------------------
-- fs_month_winrate
-- --------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fs_month_winrate(p_id int, p_month int, p_years int)
RETURNS double precision
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
WITH span AS (
  SELECT (current_date - make_interval(years => p_years))::date AS dt_from
),
m AS (
  SELECT *
  FROM public.fs_matches_long f
  JOIN span s ON f.match_date >= s.dt_from
  WHERE f.player_id = p_id
    AND EXTRACT(MONTH FROM f.match_date) = p_month
),
agg AS (
  SELECT
    COUNT(*) FILTER (WHERE winner_id = player_id) AS wins,
    COUNT(*) AS played
  FROM m
)
SELECT CASE WHEN played > 0 THEN wins::double precision / played ELSE NULL::double precision END
FROM agg;
$$;

GRANT EXECUTE ON FUNCTION public.fs_month_winrate(int,int,int) TO anon, authenticated, service_role;

-- --------------------------------------------------------------------
-- fs_surface_winrate
-- --------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fs_surface_winrate(p_id int, p_surface text, p_years int)
RETURNS double precision
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
WITH span AS (
  SELECT (current_date - make_interval(years => p_years))::date AS dt_from
),
m AS (
  SELECT *
  FROM public.fs_matches_long f
  JOIN span s ON f.match_date >= s.dt_from
  WHERE f.player_id = p_id
    AND lower(coalesce(f.surface,'')) = lower(coalesce(p_surface,''))
),
agg AS (
  SELECT
    COUNT(*) FILTER (WHERE winner_id = player_id) AS wins,
    COUNT(*) AS played
  FROM m
)
SELECT CASE WHEN played > 0 THEN wins::double precision / played ELSE NULL::double precision END
FROM agg;
$$;

GRANT EXECUTE ON FUNCTION public.fs_surface_winrate(int,text,int) TO anon, authenticated, service_role;

-- --------------------------------------------------------------------
-- fs_speed_winrate  (ahora usa court_speed_rankig_norm_compat)
-- --------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.fs_speed_winrate(p_id int, p_speed text, p_years int)
RETURNS double precision
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
WITH span AS (
  SELECT (current_date - make_interval(years => p_years))::date AS dt_from
),
mx AS (
  SELECT
    f.*,
    c.speed_bucket,
    c.speed_rank
  FROM public.fs_matches_long f
  LEFT JOIN public.court_speed_rankig_norm_compat c
    ON lower(f.tournament_name) = lower(c.tournament_name)
  JOIN span s ON f.match_date >= s.dt_from
  WHERE f.player_id = p_id
),
mx2 AS (
  SELECT
    *,
    COALESCE(
      speed_bucket,
      CASE
        WHEN speed_rank IS NULL THEN NULL
        WHEN speed_rank <= 33 THEN 'Slow'
        WHEN speed_rank <= 66 THEN 'Medium'
        ELSE 'Fast'
      END
    ) AS sb
  FROM mx
),
m AS (
  SELECT *
  FROM mx2
  WHERE lower(sb) = lower(p_speed)
),
agg AS (
  SELECT
    COUNT(*) FILTER (WHERE winner_id = player_id) AS wins,
    COUNT(*) AS played
  FROM m
)
SELECT CASE WHEN played > 0 THEN wins::double precision / played ELSE NULL::double precision END
FROM agg;
$$;

GRANT EXECUTE ON FUNCTION public.fs_speed_winrate(int,text,int) TO anon, authenticated, service_role;
