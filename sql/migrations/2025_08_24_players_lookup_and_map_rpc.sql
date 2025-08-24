-- === Asegurar columna en estratego_v1.players ===
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema='estratego_v1'
       AND table_name='players'
       AND column_name='ext_sportradar_id'
  ) THEN
    ALTER TABLE estratego_v1.players
      ADD COLUMN ext_sportradar_id text;
    COMMENT ON COLUMN estratego_v1.players.ext_sportradar_id
      IS 'Sportradar competitor numeric id (sin prefijo sr:competitor:). Ej: 225050';
  END IF;
END $$;

-- Índice único parcial (solo cuando no es NULL)
CREATE UNIQUE INDEX IF NOT EXISTS estratego_v1_players_ext_srid_uidx
  ON estratego_v1.players (ext_sportradar_id)
  WHERE ext_sportradar_id IS NOT NULL;

-- === Vista pública para leer (nombre/uuid/srid) ===
CREATE OR REPLACE VIEW public.players_lookup AS
SELECT
  p.player_id,
  p.name,
  p.ext_sportradar_id
FROM estratego_v1.players p;

COMMENT ON VIEW public.players_lookup
  IS 'Readable view: player_id, name, ext_sportradar_id desde estratego_v1.players';

-- Permisos de lectura (con service_role vale, pero abrimos lectura por si usas anon/auth en tests)
GRANT SELECT ON public.players_lookup TO anon, authenticated, service_role;

-- === RPC: mapear SRID -> UUID en estratego_v1.players ===
CREATE OR REPLACE FUNCTION public.map_sportradar_id(p_uuid uuid, p_srid text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = estratego_v1, public
AS $$
BEGIN
  IF p_uuid IS NULL OR p_srid IS NULL OR length(p_srid)=0 THEN
    RAISE EXCEPTION 'p_uuid y p_srid obligatorios';
  END IF;

  -- limpiar colisiones previas del mismo SRID
  UPDATE estratego_v1.players
    SET ext_sportradar_id = NULL
  WHERE ext_sportradar_id = p_srid
    AND player_id <> p_uuid;

  -- asignar al jugador objetivo
  UPDATE estratego_v1.players
    SET ext_sportradar_id = p_srid
  WHERE player_id = p_uuid;
END;
$$;

GRANT EXECUTE ON FUNCTION public.map_sportradar_id(uuid, text) TO anon, authenticated, service_role;

-- === RPC: buscar jugadores por nombre (en estratego_v1.players) ===
CREATE OR REPLACE FUNCTION public.find_players_by_name(p_name text)
RETURNS TABLE (player_id uuid, name text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = estratego_v1, public
AS $$
  SELECT player_id, name
  FROM estratego_v1.players
  WHERE name ILIKE '%'||coalesce(p_name,'')||'%'
  ORDER BY name
  LIMIT 10
$$;

GRANT EXECUTE ON FUNCTION public.find_players_by_name(text) TO anon, authenticated, service_role;
