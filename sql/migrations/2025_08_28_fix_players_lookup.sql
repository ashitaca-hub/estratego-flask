-- 2025_08_28_fix_players_lookup.sql
-- (Re)crea la vista players_lookup uniendo estratego_v1.players con public.players_ext.
-- Si existe public.players_min, también lo añade por compatibilidad.

-- Seguridad: asegúrate de que existe la tabla de mapping
CREATE TABLE IF NOT EXISTS public.players_ext(
  player_id int PRIMARY KEY,
  ext_sportradar_id text UNIQUE
);

DO $$
DECLARE
  has_min boolean;
BEGIN
  SELECT EXISTS (
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name='players_min'
  ) INTO has_min;

  IF has_min THEN
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW public.players_lookup AS
      SELECT
        p.player_id::int AS player_id,
        p.name::text     AS name,
        e.ext_sportradar_id::text AS ext_sportradar_id
      FROM estratego_v1.players p
      LEFT JOIN public.players_ext e USING (player_id)
      UNION
      SELECT
        pm.player_id::int AS player_id,
        pm.name::text     AS name,
        e.ext_sportradar_id::text AS ext_sportradar_id
      FROM public.players_min pm
      LEFT JOIN public.players_ext e USING (player_id);
    $SQL$;
  ELSE
    EXECUTE $SQL$
      CREATE OR REPLACE VIEW public.players_lookup AS
      SELECT
        p.player_id::int AS player_id,
        p.name::text     AS name,
        e.ext_sportradar_id::text AS ext_sportradar_id
      FROM estratego_v1.players p
      LEFT JOIN public.players_ext e USING (player_id);
    $SQL$;
  END IF;
END $$;

COMMENT ON VIEW public.players_lookup
IS 'Players + Sportradar mapping (estratego_v1.players ∪ public.players_min)';
GRANT SELECT ON public.players_lookup TO anon, authenticated, service_role;
