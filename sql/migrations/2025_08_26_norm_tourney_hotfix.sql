-- Hotfix: asegurar que unaccent se resuelve (public o extensions)
CREATE OR REPLACE FUNCTION public.norm_tourney(txt text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = public, extensions, pg_temp
AS $$
  SELECT CASE
           WHEN $1 IS NULL THEN NULL
           ELSE trim(regexp_replace(lower(unaccent($1)), '[^a-z0-9]+', ' ', 'g'))
         END
$$;

-- Refresca sugerencias (si existen)
DO $$
BEGIN
  IF to_regclass('public.tourney_key_suggestions') IS NOT NULL THEN
    REFRESH MATERIALIZED VIEW public.tourney_key_suggestions;
  END IF;
END$$;

-- Sanity: localizar dónde vive unaccent y probar la función
SELECT n.nspname AS schema, p.proname, pg_get_function_identity_arguments(p.oid) AS args
FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
WHERE p.proname='unaccent';

SELECT public.norm_tourney('Western & Southern Open (Cincinnati)') AS sample_norm LIMIT 1;
