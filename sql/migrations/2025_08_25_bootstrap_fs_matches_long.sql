-- Bootstrap robusto de la vista canónica public.fs_matches_long
-- Detecta columnas reales de estratego_v1.matches y construye SELECT dinámico.

DO $$
DECLARE
  v_schema text := 'estratego_v1';
  v_table  text := 'matches';
  v_has    boolean;

  v_cols   text[];

  -- columnas candidatas
  c_date   text[];
  c_tourn  text[];
  c_surf   text[];
  c_p1     text[];
  c_p2     text[];
  c_win    text[];

  col_date text;
  col_tour text;
  col_surf text;
  col_p1   text;
  col_p2   text;
  col_win  text;

  has_long boolean; -- player_id/opponent_id ya existen
  src_sql  text;
BEGIN
  -- ¿Existe la tabla?
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema=v_schema AND table_name=v_table
  ) INTO v_has;

  IF NOT v_has THEN
    -- Si no existe, crea una vista vacía con el esquema correcto (no rompe migraciones)
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.fs_matches_long AS
      SELECT
        NULL::date AS match_date,
        NULL::int  AS player_id,
        NULL::int  AS opponent_id,
        NULL::int  AS winner_id,
        NULL::text AS tournament_name,
        NULL::text AS surface
      WHERE false
    $q$;
    COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica para FS (tabla fuente no encontrada)';
    GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
    RETURN;
  END IF;

  -- Lista de columnas en minúsculas
  SELECT array_agg(lower(column_name)::text)
  INTO v_cols
  FROM information_schema.columns
  WHERE table_schema=v_schema AND table_name=v_table;

  -- Candidatos por campo
  c_date := ARRAY['match_date','start_time','event_date','start_at','date','startdate','started_at'];
  c_tourn:= ARRAY['tournament_name','competition_name','event_name','tournament'];
  c_surf := ARRAY['surface','court_surface','court','surface_name'];
  c_p1   := ARRAY['player1_id','player_1_id','home_player_id','player_a_id','p1_id'];
  c_p2   := ARRAY['player2_id','player_2_id','away_player_id','player_b_id','p2_id'];
  c_win  := ARRAY['winner_id','winner_player_id','winner','winnerid'];

  -- Elegir la primera que exista
  SELECT x FROM unnest(c_date) x WHERE x = ANY (v_cols) LIMIT 1 INTO col_date;
  SELECT x FROM unnest(c_tourn) x WHERE x = ANY (v_cols) LIMIT 1 INTO col_tour;
  SELECT x FROM unnest(c_surf) x WHERE x = ANY (v_cols) LIMIT 1 INTO col_surf;
  SELECT x FROM unnest(c_p1) x   WHERE x = ANY (v_cols) LIMIT 1 INTO col_p1;
  SELECT x FROM unnest(c_p2) x   WHERE x = ANY (v_cols) LIMIT 1 INTO col_p2;
  SELECT x FROM unnest(c_win) x  WHERE x = ANY (v_cols) LIMIT 1 INTO col_win;

  -- ¿Está ya en formato largo?
  has_long := ( 'player_id' = ANY(v_cols) AND 'opponent_id' = ANY(v_cols) );

  IF has_long THEN
    -- Formato largo: seleccionar columnas (usando NULL si faltan algunas opcionales)
    src_sql := format(
      'SELECT %s AS match_date, %s AS tournament_name, %s AS surface, %s AS player_id, %s AS opponent_id, %s AS winner_id FROM %I.%I',
      COALESCE(format('%I::date', col_date), 'NULL::date'),
      COALESCE(format('%I::text', col_tour), 'NULL::text'),
      COALESCE(format('lower(%I::text)', col_surf), 'NULL::text'),
      'player_id::int',
      'opponent_id::int',
      COALESCE(format('%I::int', col_win), 'NULL::int'),
      v_schema, v_table
    );
  ELSE
    -- Formato ancho: necesitamos p1/p2; si no existen, creamos vista vacía
    IF col_p1 IS NULL OR col_p2 IS NULL THEN
      EXECUTE $q$
        CREATE OR REPLACE VIEW public.fs_matches_long AS
        SELECT
          NULL::date AS match_date,
          NULL::int  AS player_id,
          NULL::int  AS opponent_id,
          NULL::int  AS winner_id,
          NULL::text AS tournament_name,
          NULL::text AS surface
        WHERE false
      $q$;
      COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica (no se detectaron columnas p1/p2 en la tabla fuente)';
      GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
      RETURN;
    END IF;

    src_sql := format(
      $fmt$
      WITH base AS (
        SELECT
          %s AS match_date,
          %s AS tournament_name,
          %s AS surface,
          %I::int AS p1,
          %I::int AS p2,
          %s AS winner_id
        FROM %I.%I m
      ),
      long AS (
        SELECT match_date, tournament_name, surface, p1 AS player_id, p2 AS opponent_id, winner_id FROM base
        UNION ALL
        SELECT match_date, tournament_name, surface, p2 AS player_id, p1 AS opponent_id, winner_id FROM base
      )
      SELECT match_date, player_id, opponent_id, winner_id, tournament_name, surface FROM long
      $fmt$,
      COALESCE(format('m.%I::date', col_date), 'NULL::date'),
      COALESCE(format('m.%I::text', col_tour), 'NULL::text'),
      COALESCE(format('lower(m.%I::text)', col_surf), 'NULL::text'),
      col_p1, col_p2,
      COALESCE(format('m.%I::int', col_win), 'NULL::int'),
      v_schema, v_table
    );
  END IF;

  -- Crear/Reemplazar la vista final
  EXECUTE 'CREATE OR REPLACE VIEW public.fs_matches_long AS ' || src_sql;
  COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica (1 fila por jugador/partido) usada por el Feature Store histórico';
  GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
END$$;
