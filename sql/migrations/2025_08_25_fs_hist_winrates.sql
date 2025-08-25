-- FS histórico v2: usa compat view y fuzzy join por torneo; deriva surface si falta

DROP FUNCTION IF EXISTS public.fs_month_winrate(int,int,int);
DROP FUNCTION IF EXISTS public.fs_surface_winrate(int,text,int);
DROP FUNCTION IF EXISTS public.fs_speed_winrate(int,text,int);

-- 1) Winrate por mes
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

-- 2) Winrate por superficie (deriva surface desde speed table si falta)
CREATE OR REPLACE FUNCTION public.fs_surface_winrate(p_id int, p_surface text, p_years int)
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
    -- LATERAL fuzzy match a la tabla de velocidades
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
  JOIN span s ON f.match_date >= s.dt_from
  WHERE f.player_id = p_id
),
mx2 AS (
  SELECT
    *,
    COALESCE(lower(f.surface), lower(c_surface)) AS surf_eff
  FROM mx f
),
m AS (
  SELECT *
  FROM mx2
  WHERE lower(coalesce(surf_eff,'')) = lower(coalesce(p_surface,''))
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

-- 3) Winrate por velocidad (usa compat + fuzzy join, bucket derivado si falta)
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
  LEFT JOIN LATERAL (
    SELECT c.*
    FROM public.court_speed_rankig_norm_compat c
    WHERE lower(c.tournament_name) = lower(f.tournament_name)
       OR lower(f.tournament_name) LIKE '%'||lower(c.tournament_name)||'%'
       OR lower(c.tournament_name) LIKE '%'||lower(f.tournament_name)||'%'
    ORDER BY (lower(c.tournament_name) = lower(f.tournament_name)) DESC, char_length(c.tournament_name) DESC
    LIMIT 1
  ) c ON true
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
