-- Winrates históricos evaluados "as-of" (antes del partido), ventana [as_of - years, as_of)
-- Requiere: public.fs_matches_long y public.court_speed_rankig_norm_compat

-- Limpieza idempotente
DROP FUNCTION IF EXISTS public.fs_month_winrate_asof(int,int,int,date);
DROP FUNCTION IF EXISTS public.fs_surface_winrate_asof(int,text,int,date);
DROP FUNCTION IF EXISTS public.fs_speed_winrate_asof(int,text,int,date);

-- 1) Mes
CREATE OR REPLACE FUNCTION public.fs_month_winrate_asof(p_id int, p_month int, p_years int, p_asof date)
RETURNS double precision
LANGUAGE sql STABLE SECURITY DEFINER AS $$
WITH span AS (
  SELECT (p_asof - make_interval(years => p_years))::date AS dt_from
),
m AS (
  SELECT *
  FROM public.fs_matches_long f, span s
  WHERE f.player_id = p_id
    AND f.match_date >= s.dt_from
    AND f.match_date <  p_asof
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
GRANT EXECUTE ON FUNCTION public.fs_month_winrate_asof(int,int,int,date) TO anon, authenticated, service_role;

-- 2) Superficie (coalesce con surface de la tabla de velocidades si falta)
CREATE OR REPLACE FUNCTION public.fs_surface_winrate_asof(p_id int, p_surface text, p_years int, p_asof date)
RETURNS double precision
LANGUAGE sql STABLE SECURITY DEFINER AS $$
WITH span AS (
  SELECT (p_asof - make_interval(years => p_years))::date AS dt_from
),
mx AS (
  SELECT
    f.*,
    c.surface AS c_surface
  FROM public.fs_matches_long f
  LEFT JOIN LATERAL (
    SELECT c.*
    FROM public.court_speed_rankig_norm_compat c
    WHERE lower(c.tournament_name) = lower(f.tournament_name)
       OR lower(f.tournament_name) LIKE '%'||lower(c.tournament_name)||'%'
       OR lower(c.tournament_name) LIKE '%'||lower(f.tournament_name)||'%'
    ORDER BY (lower(c.tournament_name) = lower(f.tournament_name)) DESC, char_length(c.tournament_name) DESC
    LIMIT 1
  ) c ON true
  , span s
  WHERE f.player_id = p_id
    AND f.match_date >= s.dt_from
    AND f.match_date <  p_asof
),
m AS (
  SELECT *, COALESCE(lower(surface), lower(c_surface)) AS surf_eff
  FROM mx
)
,
agg AS (
  SELECT
    COUNT(*) FILTER (WHERE winner_id = player_id) AS wins,
    COUNT(*) AS played
  FROM m
  WHERE lower(coalesce(surf_eff,'')) = lower(coalesce(p_surface,''))
)
SELECT CASE WHEN played > 0 THEN wins::double precision / played ELSE NULL::double precision END
FROM agg;
$$;
GRANT EXECUTE ON FUNCTION public.fs_surface_winrate_asof(int,text,int,date) TO anon, authenticated, service_role;

-- 3) Velocidad (bucket derivado si falta)
CREATE OR REPLACE FUNCTION public.fs_speed_winrate_asof(p_id int, p_speed text, p_years int, p_asof date)
RETURNS double precision
LANGUAGE sql STABLE SECURITY DEFINER AS $$
WITH span AS (
  SELECT (p_asof - make_interval(years => p_years))::date AS dt_from
),
mx AS (
  SELECT
    f.*,
    c.speed_bucket,
    c.speed_rank
  FROM public.fs_matches_long f
  LEFT JOIN LATERAL (
    SELECT c.*
    FROM public.court_speed_rankig_norm_compat c
    WHERE lower(c.tournament_name) = lower(f.tournament_name)
       OR lower(f.tournament_name) LIKE '%'||lower(c.tournament_name)||'%'
       OR lower(c.tournament_name) LIKE '%'||lower(f.tournament_name)||'%'
    ORDER BY (lower(c.tournament_name) = lower(f.tournament_name)) DESC, char_length(c.tournament_name) DESC
    LIMIT 1
  ) c ON true
  , span s
  WHERE f.player_id = p_id
    AND f.match_date >= s.dt_from
    AND f.match_date <  p_asof
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
agg AS (
  SELECT
    COUNT(*) FILTER (WHERE winner_id = player_id) AS wins,
    COUNT(*) AS played
  FROM mx2
  WHERE lower(sb) = lower(p_speed)
)
SELECT CASE WHEN played > 0 THEN wins::double precision / played ELSE NULL::double precision END
FROM agg;
$$;
GRANT EXECUTE ON FUNCTION public.fs_speed_winrate_asof(int,text,int,date) TO anon, authenticated, service_role;
