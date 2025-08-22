-- Asegurar columna ext_sportradar_id en public.players + índice único parcial
-- Ejecutar con una clave service_role (DATABASE_URL)

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name   = 'players'
      AND column_name  = 'ext_sportradar_id'
  ) THEN
    ALTER TABLE public.players
      ADD COLUMN ext_sportradar_id text;

    COMMENT ON COLUMN public.players.ext_sportradar_id
      IS 'Sportradar competitor numeric id (texto, sin prefijo sr:competitor:). Ej: 225050';
  END IF;
END $$;

-- Índice único (solo cuando no es NULL)
CREATE UNIQUE INDEX IF NOT EXISTS players_ext_srid_uidx
  ON public.players (ext_sportradar_id)
  WHERE ext_sportradar_id IS NOT NULL;
