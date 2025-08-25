-- Crea/actualiza una vista de compatibilidad que siempre expone speed_bucket (o NULL)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name='court_speed_rankig_norm'
      AND column_name='speed_bucket'
  ) THEN
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.court_speed_rankig_norm_compat AS
      SELECT tournament_name, surface, speed_rank, speed_bucket
      FROM public.court_speed_rankig_norm
    $q$;
  ELSE
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.court_speed_rankig_norm_compat AS
      SELECT tournament_name, surface, speed_rank, NULL::text AS speed_bucket
      FROM public.court_speed_rankig_norm
    $q$;
  END IF;

  COMMENT ON VIEW public.court_speed_rankig_norm_compat
    IS 'Compat: expone speed_bucket (o NULL) + speed_rank para court_speed_rankig_norm';
  GRANT SELECT ON public.court_speed_rankig_norm_compat TO anon, authenticated, service_role;
END$$;

-- Re-crear fs_speed_winrate para usar la vista _compat (no falla si falta speed_bucket real)
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
