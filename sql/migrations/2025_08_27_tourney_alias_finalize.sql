-- Asegura extensiones necesarias
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- norm_tourney ya existe con search_path fijado; lo dejamos tal cual

-- Vistas keyed (recreables e idempotentes)
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

CREATE OR REPLACE VIEW public.fs_matches_long_keyed AS
SELECT
  f.*,
  public.norm_tourney(f.tournament_name) AS tourney_key
FROM public.fs_matches_long f;
GRANT SELECT ON public.fs_matches_long_keyed TO anon, authenticated, service_role;

-- Mapa de alias (por si no existía)
CREATE TABLE IF NOT EXISTS public.tourney_key_map (
  src_key  text PRIMARY KEY,
  dest_key text NOT NULL,
  note     text
);
CREATE INDEX IF NOT EXISTS tourney_key_map_dest_idx ON public.tourney_key_map(dest_key);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.tourney_key_map TO authenticated, service_role;

-- Resolver final
CREATE OR REPLACE VIEW public.tourney_speed_resolved AS
SELECT k.tourney_key, k.surface, k.speed_rank, k.speed_bucket
FROM public.court_speed_rankig_norm_compat_keyed k
UNION
SELECT m.src_key AS tourney_key, k.surface, k.speed_rank, k.speed_bucket
FROM public.tourney_key_map m
JOIN public.court_speed_rankig_norm_compat_keyed k
  ON k.tourney_key = m.dest_key;
GRANT SELECT ON public.tourney_speed_resolved TO anon, authenticated, service_role;

-- Crear materialized view de sugerencias si falta; si existe, no la tocamos
DO $$
BEGIN
  IF to_regclass('public.tourney_key_suggestions') IS NULL THEN
    CREATE MATERIALIZED VIEW public.tourney_key_suggestions AS
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
    GRANT SELECT ON public.tourney_key_suggestions TO anon, authenticated, service_role;
  END IF;
END$$;

-- Refrescar sugerencias (ya exista o acabe de crearse)
DO $$
BEGIN
  IF to_regclass('public.tourney_key_suggestions') IS NOT NULL THEN
    REFRESH MATERIALIZED VIEW public.tourney_key_suggestions;
  END IF;
END$$;

-- Vista de validación de alias huérfanos
CREATE OR REPLACE VIEW public.tourney_key_map_orphans AS
SELECT m.*
FROM public.tourney_key_map m
LEFT JOIN public.court_speed_rankig_norm_compat_keyed k ON k.tourney_key = m.dest_key
WHERE k.tourney_key IS NULL;
GRANT SELECT ON public.tourney_key_map_orphans TO anon, authenticated, service_role;
