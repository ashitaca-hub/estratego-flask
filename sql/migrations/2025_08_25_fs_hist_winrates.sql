-- --------------------------------------------------------------------
-- Requisitos:
--   - Vista pública canónica: public.fs_matches_long con columnas:
--       match_date::date
--       player_id::int
--       opponent_id::int
--       winner_id::int
--       tournament_name::text
--       surface::text   -- valores tipo 'hard'/'clay'/'grass' (insensible a mayús)
--   - Vista de velocidades: public.court_speed_rankig_norm
--       tournament_name, surface, speed_rank (num opc), speed_bucket (opc)
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
    -- emparejar torneo con tabla de velocidades (igualdad case-insensitive; ajusta si necesitas ILIKE flexible)
    c.speed_bucket,
    c.speed_rank
  FROM public.fs_matches_long f
  LEFT JOIN public.court_speed_rankig_norm c
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
