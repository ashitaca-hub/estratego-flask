-- Bootstrap v3.1: crea/actualiza public.fs_matches_long con autodetección robusta
DO $$
DECLARE
  schemas text[] := ARRAY['estratego_v1','public'];

  rec record;
  cols text[];

  -- candidatos por campo
  c_date   text[] := ARRAY['match_date','start_time','event_date','start_at','date','startdate','started_at'];
  c_tourn  text[] := ARRAY['tournament_name','competition_name','event_name','tournament'];
  c_surf   text[] := ARRAY['surface','court_surface','court','surface_name'];

  -- por tabla (SIEMPRE RESET)
  has_long boolean;
  has_wide boolean;
  col_date text; col_tour text; col_surf text;
  col_p1   text; col_p2   text; col_win  text;

  part_sql  text;
  union_sql text := '';
BEGIN
  FOR rec IN
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = ANY(schemas) AND table_type='BASE TABLE'
    ORDER BY table_schema, table_name
  LOOP
    -- reset por tabla
    cols := NULL;
    has_long := false; has_wide := false;
    col_date := NULL; col_tour := NULL; col_surf := NULL;
    col_p1 := NULL; col_p2 := NULL; col_win := NULL;

    -- columnas de la tabla
    SELECT array_agg(lower(column_name)::text)
    INTO cols
    FROM information_schema.columns
    WHERE table_schema=rec.table_schema AND table_name=rec.table_name;

    IF cols IS NULL THEN
      CONTINUE;
    END IF;

    -- detectar formato
    has_long := ('player_id'=ANY(cols) AND 'opponent_id'=ANY(cols));
    has_wide := (
       ('player1_id'=ANY(cols) OR 'player_1_id'=ANY(cols) OR 'home_player_id'=ANY(cols) OR 'player_a_id'=ANY(cols) OR 'p1_id'=ANY(cols))
       AND
       ('player2_id'=ANY(cols) OR 'player_2_id'=ANY(cols) OR 'away_player_id'=ANY(cols) OR 'player_b_id'=ANY(cols) OR 'p2_id'=ANY(cols))
    );

    -- columnas clave
    SELECT x FROM unnest(ARRAY['winner_id','winner_player_id','winner','winnerid']) x WHERE x = ANY(cols) LIMIT 1 INTO col_win;
    SELECT x FROM unnest(c_date)  x WHERE x = ANY(cols) LIMIT 1 INTO col_date;
    SELECT x FROM unnest(c_tourn) x WHERE x = ANY(cols) LIMIT 1 INTO col_tour;
    SELECT x FROM unnest(c_surf)  x WHERE x = ANY(cols) LIMIT 1 INTO col_surf;

    -- winner_id y fecha son obligatorias para que sume
    IF col_win IS NULL OR col_date IS NULL THEN
      CONTINUE;
    END IF;

    IF has_long THEN
      -- LARGO: player_id/opponent_id ya existen
      part_sql := format(
        'SELECT %s AS match_date, %s AS tournament_name, %s AS surface,
                %s AS player_id, %s AS opponent_id, %s AS winner_id
         FROM %I.%I',
        CASE WHEN col_date IS NOT NULL THEN format('%I::date', col_date) ELSE 'NULL::date' END,
        CASE WHEN col_tour IS NOT NULL THEN format('%I::text', col_tour) ELSE 'NULL::text' END,
        CASE WHEN col_surf IS NOT NULL THEN format('lower(%I::text)', col_surf) ELSE 'NULL::text' END,
        'player_id::int',
        'opponent_id::int',
        format('%I::int', col_win),
        rec.table_schema, rec.table_name
      );
      RAISE NOTICE 'fs_matches_long <- %.% (long)', rec.table_schema, rec.table_name;

    ELSIF has_wide THEN
      -- ANCHO: localizar p1/p2 candidatos
      SELECT x FROM unnest(ARRAY['player1_id','player_1_id','home_player_id','player_a_id','p1_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p1;
      SELECT x FROM unnest(ARRAY['player2_id','player_2_id','away_player_id','player_b_id','p2_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p2;
      IF col_p1 IS NULL OR col_p2 IS NULL THEN
        CONTINUE;
      END IF;

      part_sql := format(
        $fmt$
        WITH base AS (
          SELECT %s AS match_date,
                 %s AS tournament_name,
                 %s AS surface,
                 %s AS p1,
                 %s AS p2,
                 %s AS winner_id
          FROM %I.%I
        )
        SELECT match_date, p1 AS player_id, p2 AS opponent_id, winner_id, tournament_name, surface FROM base
        UNION ALL
        SELECT match_date, p2 AS player_id, p1 AS opponent_id, winner_id, tournament_name, surface FROM base
        $fmt$,
        CASE WHEN col_date IS NOT NULL THEN format('%I::date', col_date) ELSE 'NULL::date' END,
        CASE WHEN col_tour IS NOT NULL THEN format('%I::text', col_tour) ELSE 'NULL::text' END,
        CASE WHEN col_surf IS NOT NULL THEN format('lower(%I::text)', col_surf) ELSE 'NULL::text' END,
        format('%I::int', col_p1),
        format('%I::int', col_p2),
        format('%I::int', col_win),
        rec.table_schema, rec.table_name
      );
      RAISE NOTICE 'fs_matches_long <- %.% (wide)', rec.table_schema, rec.table_name;

    ELSE
      CONTINUE;
    END IF;

    -- acumular
    IF union_sql = '' THEN
      union_sql := part_sql;
    ELSE
      union_sql := union_sql || ' UNION ALL ' || part_sql;
    END IF;
  END LOOP;

  IF union_sql = '' THEN
    -- placeholder vacía
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
  ELSE
    EXECUTE 'CREATE OR REPLACE VIEW public.fs_matches_long AS ' || union_sql;
  END IF;

  COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica auto-generada desde tablas de partidos (1 fila por jugador/partido).';
  GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
END$$;
