CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Normalizador (ya lo tienes, pero lo dejamos idempotente)
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

-- Vista keyed de velocidades (deriva bucket de rank)
CREATE OR REPLACE VIEW public.court_speed_rankig_norm_compat_keyed AS
SELECT
  tournament_name,
  public.norm_tourney(tournament_name) AS tourney_key,
  lower(surface) AS surface,
  speed_rank,
  CASE
    WHEN speed_rank IS NULL THEN NULL
    WHEN speed_rank <= 33 THEN 'Slow'
    WHEN speed_rank <= 66 THEN 'Medium'
    ELSE 'Fast'
  END AS speed_bucket
FROM public.court_speed_rankig_norm;

GRANT SELECT ON public.court_speed_rankig_norm_compat_keyed TO anon, authenticated, service_role;

-- Vista keyed de tus partidos
CREATE OR REPLACE VIEW public.fs_matches_long_keyed AS
SELECT f.*,
       public.norm_tourney(f.tournament_name) AS tourney_key
FROM public.fs_matches_long f;

GRANT SELECT ON public.fs_matches_long_keyed TO anon, authenticated, service_role;

-- Mapa de alias: src_key (como viene en tus datos) -> dest_key (como está en la tabla de velocidades)
CREATE TABLE IF NOT EXISTS public.tourney_key_map (
  src_key  text PRIMARY KEY,
  dest_key text NOT NULL REFERENCES public.court_speed_rankig_norm_compat_keyed(tourney_key),
  note     text
);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.tourney_key_map TO authenticated, service_role;

-- Resolver final: combina claves nativas y alias
CREATE OR REPLACE VIEW public.tourney_speed_resolved AS
SELECT k.tourney_key, k.surface, k.speed_rank, k.speed_bucket
FROM public.court_speed_rankig_norm_compat_keyed k
UNION
SELECT m.src_key AS tourney_key, k.surface, k.speed_rank, k.speed_bucket
FROM public.tourney_key_map m
JOIN public.court_speed_rankig_norm_compat_keyed k
  ON k.tourney_key = m.dest_key;

GRANT SELECT ON public.tourney_speed_resolved TO anon, authenticated, service_role;

-- Sugerencias automáticas (para poblar alias fácilmente)
CREATE MATERIALIZED VIEW IF NOT EXISTS public.tourney_key_suggestions AS
WITH fs AS (
  SELECT DISTINCT tourney_key
  FROM public.fs_matches_long_keyed
  WHERE tourney_key IS NOT NULL
),
unmatched AS (
  SELECT fs.tourney_key
  FROM fs
  LEFT JOIN public.court_speed_rankig_norm_compat_keyed k ON k.tourney_key = fs.tourney_key
  LEFT JOIN public.tourney_key_map m ON m.src_key = fs.tourney_key
  WHERE k.tourney_key IS NULL AND m.src_key IS NULL
)
SELECT u.tourney_key AS src_key,
       k.tourney_key AS candidate_key,
       similarity(u.tourney_key, k.tourney_key) AS sim
FROM unmatched u
CROSS JOIN public.court_speed_rankig_norm_compat_keyed k
WHERE similarity(u.tourney_key, k.tourney_key) >= 0.30
ORDER BY u.tourney_key, sim DESC;

CREATE INDEX IF NOT EXISTS tourney_key_suggestions_src ON public.tourney_key_suggestions (src_key);

-- Refresca sugerencias
DO $$
BEGIN
  IF to_regclass('public.tourney_key_suggestions') IS NOT NULL THEN
    REFRESH MATERIALIZED VIEW public.tourney_key_suggestions;
  END IF;
END$$;
