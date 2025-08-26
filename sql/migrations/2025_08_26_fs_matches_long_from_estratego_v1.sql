DO $$
DECLARE
  cols text[];
  has_table boolean;

  -- detectores
  col_date text;
  col_tdate text;
  col_p1 text; col_p2 text;
  col_win text; col_lose text;
  col_surface text;
  col_tname text;
  has_long boolean;  -- player1/player2
  has_wl   boolean;  -- winner/loser
  has_tourn_table boolean;

  date_expr text;
  tname_expr text;
  from_clause text;
BEGIN
  -- 0) ¿Existe la tabla?
  SELECT EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema='estratego_v1' AND table_name='matches'
  ) INTO has_table;

  IF NOT has_table THEN
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.fs_matches_long AS
      SELECT NULL::date AS match_date, NULL::int AS player_id, NULL::int AS opponent_id,
             NULL::int AS winner_id, NULL::text AS tournament_name, NULL::text AS surface
      WHERE false
    $q$;
    COMMENT ON VIEW public.fs_matches_long IS 'FS: fuente estratego_v1.matches NO encontrada (placeholder vacío).';
    GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
    RETURN;
  END IF;

  -- 1) columnas presentes
  SELECT array_agg(lower(column_name)::text)
  INTO cols
  FROM information_schema.columns
  WHERE table_schema='estratego_v1' AND table_name='matches';

  -- 2) fecha
  -- preferimos match_date/date/start_time/event_date/start_at/started_at; si no, 'tourney_date' (YYYYMMDD)
  SELECT x FROM unnest(ARRAY['match_date','date','start_time','event_date','start_at','started_at']) x
   WHERE x = ANY(cols) LIMIT 1 INTO col_date;
  IF col_date IS NOT NULL THEN
    date_expr := format('m.%I::date', col_date);
  ELSIF 'tourney_date' = ANY(cols) THEN
    -- si viene numérico YYYYMMDD
    date_expr := 'CASE WHEN m.tourney_date IS NULL THEN NULL::date
                       WHEN pg_typeof(m.tourney_date)::text IN (''integer'',''bigint'',''numeric'')
                         THEN to_date(m.tourney_date::text, ''YYYYMMDD'')
                       ELSE to_date(m.tourney_date::text, ''YYYYMMDD'') END';
  ELSE
    date_expr := 'NULL::date';
  END IF;

  -- 3) jugadores
  has_long := ( 'player1_id' = ANY(cols) OR 'player_1_id' = ANY(cols) OR 'p1_id' = ANY(cols) )
              AND ( 'player2_id' = ANY(cols) OR 'player_2_id' = ANY(cols) OR 'p2_id' = ANY(cols) );

  IF has_long THEN
    SELECT x FROM unnest(ARRAY['player1_id','player_1_id','p1_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p1;
    SELECT x FROM unnest(ARRAY['player2_id','player_2_id','p2_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p2;
  END IF;

  has_wl := ( 'winner_id' = ANY(cols) ) AND ( 'loser_id' = ANY(cols) );
  IF has_wl THEN
    col_win := 'winner_id';
    col_lose := 'loser_id';
  END IF;

  IF NOT has_long AND NOT has_wl THEN
    RAISE NOTICE 'estratego_v1.matches no tiene ni (p1/p2) ni (winner/loser) → placeholder vacío';
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.fs_matches_long AS
      SELECT NULL::date AS match_date, NULL::int AS player_id, NULL::int AS opponent_id,
             NULL::int AS winner_id, NULL::text AS tournament_name, NULL::text AS surface
      WHERE false
    $q$;
    RETURN;
  END IF;

  -- 4) surface
  IF 'surface' = ANY(cols) THEN
    col_surface := 'surface';
  ELSE
    col_surface := NULL;
  END IF;

  -- 5) tournament name directo (si existe)
  SELECT x FROM unnest(ARRAY['tournament_name','tourney_name','event_name','competition_name']) x
   WHERE x = ANY(cols) LIMIT 1 INTO col_tname;

  -- 6) ¿existe tabla de torneos para mapear tourney_id -> nombre?
  SELECT to_regclass('estratego_v1.tournaments') IS NOT NULL INTO has_tourn_table;

  -- 7) construir FROM / expresión de torneo
  IF col_tname IS NOT NULL THEN
    tname_expr := format('lower(m.%I::text)', col_tname);
    from_clause := ' FROM estratego_v1.matches m ';
  ELSIF has_tourn_table AND 'tourney_id' = ANY(cols) THEN
    -- buscar columnas candidatas en estratego_v1.tournaments
    PERFORM 1;
    tname_expr := 'lower(t.tourney_name::text)';
    from_clause := ' FROM estratego_v1.matches m LEFT JOIN estratego_v1.tournaments t ON t.tourney_id = m.tourney_id ';
  ELSE
    tname_expr := 'NULL::text';
    from_clause := ' FROM estratego_v1.matches m ';
  END IF;

  -- 8) crear vista en formato LARGO (2 filas por partido)
  EXECUTE '
    CREATE OR REPLACE VIEW public.fs_matches_long AS
    WITH base AS (
      SELECT
        '|| date_expr ||'                                             AS match_date,
        '|| tname_expr ||'                                            AS tournament_name,
        '|| CASE WHEN col_surface IS NOT NULL THEN format('lower(m.%I::text)', col_surface) ELSE 'NULL::text' END ||' AS surface,
        '|| CASE
              WHEN has_long THEN format('m.%I::int', col_p1)
              ELSE format('m.%I::int', col_win)
            END ||'                                                   AS p_a,
        '|| CASE
              WHEN has_long THEN format('m.%I::int', col_p2)
              ELSE format('m.%I::int', col_lose)
            END ||'                                                   AS p_b,
        '|| CASE WHEN has_wl THEN format('m.%I::int', col_win) ELSE 'NULL::int' END ||'  AS winner
      '|| from_clause ||'
    )
    SELECT match_date, p_a AS player_id, p_b AS opponent_id, winner AS winner_id, tournament_name, surface FROM base
    UNION ALL
    SELECT match_date, p_b AS player_id, p_a AS opponent_id, winner AS winner_id, tournament_name, surface FROM base
  ';

  COMMENT ON VIEW public.fs_matches_long IS 'FS: derivada de estratego_v1.matches (2 filas/partido). Usa torneo directo si existe; si no, intenta join a estratego_v1.tournaments por tourney_id; si no, queda NULL.';
  GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
END$$;
