-- Índices para acelerar consultas tipo:
--  - histórico "as-of" por jugador y fecha
--  - filtros por rango de fechas
--  - joins por torneo

DO $$
BEGIN
  -- ==== estratego_v1.matches (la que usas para construir fs_matches_long) ====
  IF to_regclass('estratego_v1.matches') IS NOT NULL THEN
    -- Si tus columnas son (winner_id, loser_id, date)
    EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_winner_date_idx ON estratego_v1.matches (winner_id, date)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_loser_date_idx  ON estratego_v1.matches (loser_id,  date)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_date_idx        ON estratego_v1.matches (date)';
    -- Si tienes surface/tourney_id y los usas aguas arriba:
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='estratego_v1' AND table_name='matches' AND column_name='tourney_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_tourney_idx   ON estratego_v1.matches (tourney_id)';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='estratego_v1' AND table_name='matches' AND column_name='surface') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_surface_idx   ON estratego_v1.matches (lower(surface))';
    END IF;
  END IF;

  -- ==== estratego_v1.tournaments (para resolver nombre -> torneo) ====
  IF to_regclass('estratego_v1.tournaments') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='estratego_v1' AND table_name='tournaments' AND column_name='name') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_tourn_name_idx ON estratego_v1.tournaments (lower(name))';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='estratego_v1' AND table_name='tournaments' AND column_name='tourney_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_tourn_id_idx   ON estratego_v1.tournaments (tourney_id)';
    END IF;
  END IF;

  -- ==== Si en algún entorno usas public.matches (uuid) ====
  IF to_regclass('public.matches') IS NOT NULL THEN
    -- Ajusta columnas si se llaman distinto (player1_id/player2_id/start_time, etc.)
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='public' AND table_name='matches' AND column_name='player1_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS pub_matches_p1_date_idx ON public.matches (player1_id, date)';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='public' AND table_name='matches' AND column_name='player2_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS pub_matches_p2_date_idx ON public.matches (player2_id, date)';
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='public' AND table_name='matches' AND column_name='date') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS pub_matches_date_idx    ON public.matches (date)';
    END IF;
  END IF;

  -- ==== Tabla/clave de velocidades de pista ====
  IF to_regclass('public.court_speed_rankig_norm_compat_keyed') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS court_speed_keyed_key_idx ON public.court_speed_rankig_norm_compat_keyed (tourney_key)';
  END IF;

END$$;

-- Actualiza estadísticas para que el planner aproveche los índices recién creados
ANALYZE;
