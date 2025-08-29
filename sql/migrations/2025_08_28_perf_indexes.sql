-- Índices de performance (versión adaptativa a tus nombres de columnas)

DO $$
DECLARE
  v_date_col     text;
  v_win_col      text;
  v_los_col      text;
  v_surface_col  text;
  v_relkind      text;
BEGIN
  ------------------------------------------------------------------
  -- estratego_v1.matches  (base de fs_matches_long en tu entorno)
  ------------------------------------------------------------------
  IF to_regclass('estratego_v1.matches') IS NOT NULL THEN
    -- Detecta la columna de fecha/tiempo
    SELECT c.column_name INTO v_date_col
    FROM information_schema.columns c
    WHERE c.table_schema='estratego_v1' AND c.table_name='matches'
      AND c.column_name IN ('match_date','date','start_time','start_at','started_at','event_date','event_time')
    ORDER BY array_position(ARRAY[
      'match_date','date','start_time','start_at','started_at','event_date','event_time'
    ]::text[], c.column_name)
    LIMIT 1;

    -- winner/loser (si existen)
    SELECT
      (SELECT column_name FROM information_schema.columns
         WHERE table_schema='estratego_v1' AND table_name='matches' AND column_name='winner_id'),
      (SELECT column_name FROM information_schema.columns
         WHERE table_schema='estratego_v1' AND table_name='matches' AND column_name='loser_id')
    INTO v_win_col, v_los_col;

    IF v_win_col IS NOT NULL AND v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS ev1_matches_winner_date_idx ON estratego_v1.matches (%I, %I)', v_win_col, v_date_col);
    END IF;
    IF v_los_col IS NOT NULL AND v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS ev1_matches_loser_date_idx  ON estratego_v1.matches (%I, %I)', v_los_col, v_date_col);
    END IF;
    IF v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS ev1_matches_date_idx ON estratego_v1.matches (%I)', v_date_col);
    END IF;

    -- surface (si existe)
    SELECT column_name INTO v_surface_col
    FROM information_schema.columns
    WHERE table_schema='estratego_v1' AND table_name='matches'
      AND column_name IN ('surface','court_surface')
    LIMIT 1;
    IF v_surface_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS ev1_matches_surface_idx ON estratego_v1.matches (lower(%I))', v_surface_col);
    END IF;

    -- tourney_id (si existe)
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='estratego_v1' AND table_name='matches' AND column_name='tourney_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS ev1_matches_tourney_idx ON estratego_v1.matches (tourney_id)';
    END IF;
  END IF;

  ------------------------------------------------------------------
  -- public.matches (si en algún entorno usas esta tabla)
  ------------------------------------------------------------------
  IF to_regclass('public.matches') IS NOT NULL THEN
    -- Detecta columnas p1/p2 y fecha
    SELECT column_name INTO v_date_col
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='matches'
      AND column_name IN ('match_date','date','start_time','started_at')
    LIMIT 1;

    SELECT column_name INTO v_win_col
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='matches'
      AND column_name IN ('player1_id','p1','player_a','player_id')
    LIMIT 1;

    SELECT column_name INTO v_los_col
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='matches'
      AND column_name IN ('player2_id','p2','player_b','opponent_id')
    LIMIT 1;

    IF v_win_col IS NOT NULL AND v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS pub_matches_p1_date_idx ON public.matches (%I, %I)', v_win_col, v_date_col);
    END IF;
    IF v_los_col IS NOT NULL AND v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS pub_matches_p2_date_idx ON public.matches (%I, %I)', v_los_col, v_date_col);
    END IF;
    IF v_date_col IS NOT NULL THEN
      EXECUTE format('CREATE INDEX IF NOT EXISTS pub_matches_date_idx ON public.matches (%I)', v_date_col);
    END IF;

    -- torneo por nombre o id
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema='public' AND table_name='matches' AND column_name='tournament_name') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS pub_matches_tname_idx ON public.matches (lower(tournament_name))';
    ELSIF EXISTS (SELECT 1 FROM information_schema.columns
                  WHERE table_schema='public' AND table_name='matches' AND column_name='tourney_id') THEN
      EXECUTE 'CREATE INDEX IF NOT EXISTS pub_matches_tourid_idx ON public.matches (tourney_id)';
    END IF;
  END IF;

  ------------------------------------------------------------------
  -- court_speed_rankig_norm_compat_keyed: sólo si es tabla o MV
  ------------------------------------------------------------------
  SELECT c.relkind INTO v_relkind
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname='public' AND c.relname='court_speed_rankig_norm_compat_keyed';

  IF v_relkind IN ('r','m') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS court_speed_keyed_key_idx ON public.court_speed_rankig_norm_compat_keyed (tourney_key)';
  ELSE
    RAISE NOTICE 'Saltando índice en public.court_speed_rankig_norm_compat_keyed (relkind=%). Es una vista normal; no indexable.', v_relkind;
  END IF;

END$$;

ANALYZE;
