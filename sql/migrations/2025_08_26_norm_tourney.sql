CREATE EXTENSION IF NOT EXISTS unaccent;

-- Normaliza nombres de torneo: minúsculas, sin acentos, solo [a-z0-9] y espacios colapsados
CREATE OR REPLACE FUNCTION public.norm_tourney(txt text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
           WHEN $1 IS NULL THEN NULL
           ELSE trim(regexp_replace(lower(unaccent($1)), '[^a-z0-9]+', ' ', 'g'))
         END
$$;

-- Vista de velocidades con clave normalizada
CREATE OR REPLACE VIEW public.court_speed_rankig_norm_compat_keyed AS
SELECT
  tournament_name,
  public.norm_tourney(tournament_name) AS tourney_key,
  lower(surface) AS surface,
  speed_rank,
  COALESCE(
    speed_bucket,
    CASE
      WHEN speed_rank IS NULL THEN NULL
      WHEN speed_rank <= 33 THEN 'Slow'
      WHEN speed_rank <= 66 THEN 'Medium'
      ELSE 'Fast'
    END
  ) AS speed_bucket
FROM public.court_speed_rankig_norm;

GRANT SELECT ON public.court_speed_rankig_norm_compat_keyed TO anon, authenticated, service_role;

-- Vista espejo de fs_matches_long con la misma clave
CREATE OR REPLACE VIEW public.fs_matches_long_keyed AS
SELECT
  f.*,
  public.norm_tourney(f.tournament_name) AS tourney_key
FROM public.fs_matches_long f;

COMMENT ON VIEW public.fs_matches_long_keyed IS 'fs_matches_long + tourney_key normalizado para joins rápidos';
GRANT SELECT ON public.fs_matches_long_keyed TO anon, authenticated, service_role;
