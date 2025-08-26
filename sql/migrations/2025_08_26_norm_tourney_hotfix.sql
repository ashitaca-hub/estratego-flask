-- Hotfix: usar extensions.unaccent en Supabase
-- (no tocamos nada más; la función se recompila y las vistas lo heredan)
CREATE OR REPLACE FUNCTION public.norm_tourney(txt text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
           WHEN $1 IS NULL THEN NULL
           ELSE trim(regexp_replace(lower(extensions.unaccent($1)), '[^a-z0-9]+', ' ', 'g'))
         END
$$;

-- Refrescar sugerencias ahora que norm_tourney funciona
DO $$
BEGIN
  IF to_regclass('public.tourney_key_suggestions') IS NOT NULL THEN
    REFRESH MATERIALIZED VIEW public.tourney_key_suggestions;
  END IF;
END$$;

-- Sanity rápido
SELECT public.norm_tourney('Western & Southern Open (Cincinnati)') AS sample_norm LIMIT 1;
