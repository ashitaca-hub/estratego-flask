-- Corrige tipos a INTEGER para player_id en las RPCs y reafirma la vista

-- 1) RPC de búsqueda por nombre (player_id INTEGER)
DROP FUNCTION IF EXISTS public.find_players_by_name(text) CASCADE;

CREATE OR REPLACE FUNCTION public.find_players_by_name(p_name text)
RETURNS TABLE (player_id integer, name text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = estratego_v1, public
AS $$
  SELECT player_id, name
  FROM estratego_v1.players
  WHERE name ILIKE '%' || coalesce(p_name,'') || '%'
  ORDER BY name
  LIMIT 10
$$;

GRANT EXECUTE ON FUNCTION public.find_players_by_name(text)
  TO anon, authenticated, service_role;

-- 2) RPC de mapeo SRID -> player_id (INTEGER)
DROP FUNCTION IF EXISTS public.map_sportradar_id(uuid, text);
DROP FUNCTION IF EXISTS public.map_sportradar_id(integer, text);

CREATE OR REPLACE FUNCTION public.map_sportradar_id(p_id integer, p_srid text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = estratego_v1, public
AS $$
BEGIN
  IF p_id IS NULL OR p_srid IS NULL OR length(p_srid)=0 THEN
    RAISE EXCEPTION 'p_id y p_srid obligatorios';
  END IF;

  -- limpiar colisiones previas del mismo SRID
  UPDATE estratego_v1.players
     SET ext_sportradar_id = NULL
   WHERE ext_sportradar_id = p_srid
     AND player_id <> p_id;

  -- asignar al jugador objetivo
  UPDATE estratego_v1.players
     SET ext_sportradar_id = p_srid
   WHERE player_id = p_id;
END;
$$;

GRANT EXECUTE ON FUNCTION public.map_sportradar_id(integer, text)
  TO anon, authenticated, service_role;

-- 3) Vista pública de consulta (sin cambios de tipo)
CREATE OR REPLACE VIEW public.players_lookup AS
SELECT p.player_id, p.name, p.ext_sportradar_id
FROM estratego_v1.players p;

GRANT SELECT ON public.players_lookup TO anon, authenticated, service_role;
