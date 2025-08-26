DO $$
DECLARE
  has_matches boolean;
  has_tourn   boolean;

  -- columnas en matches
  m_cols text[];
  m_col_date text;
  m_col_p1   text; m_col_p2 text;
  m_has_p1p2 boolean;
  m_has_wl   boolean;
  m_col_surface text;
  m_col_tname  text;

  -- columnas en tournaments
  t_cols text[];
  t_col_date  text;
  t_col_tname text;
  t_col_surface text;
  t_has_tourney_id boolean;

  -- expresiones dinámicas
  date_expr   text;
  tname_expr  text;
  surf_expr   text;
  from_clause text;
BEGIN
  -- ¿Existen las tablas?
  SELECT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='estratego_v1' AND table_name='matches')
    INTO has_matches;
  SELECT EXISTS (SELECT 1 FROM information_schema.tables
                 WHERE table_schema='estratego_v1' AND table_name='tournaments')
    INTO has_tourn;

  IF NOT has_matches THEN
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.fs_matches_long AS
      SELECT NULL::date AS match_date, NULL::int AS player_id, NULL::int AS opponent_id,
             NULL::int AS winner_id, NULL::text AS tournament_name, NULL::text AS surface
      WHERE false
    $q$;
    COMMENT ON VIEW public.fs_matches_long IS 'FS: estratego_v1.matches no existe (placeholder vacío).';
    GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
    RETURN;
  END IF;

  -- Columnas de matches
  SELECT array_agg(lower(column_name)::text)
  INTO m_cols
  FROM information_schema.columns
  WHERE table_schema='estratego_v1' AND table_name='matches';

  -- Jugadores: p1/p2 o winner/loser
  m_has_p1p2 := ( 'player1_id' = ANY(m_cols) OR 'player_1_id' = ANY(m_cols) OR 'p1_id' = ANY(m_cols) )
             AND ( 'player2_id' = ANY(m_cols) OR 'player_2_id' = ANY(m_cols) OR 'p2_id' = ANY(m_cols) );
  IF m_has_p1p2 THEN
    SELECT x FROM unnest(ARRAY['player1_id','player_1_id','p1_id']) x WHERE x = ANY(m_cols) LIMIT 1 INTO m_col_p1;
    SELECT x FROM unnest(ARRAY['player2_id','player_2_id','p2_id']) x WHERE x = ANY(m_cols) LIMIT 1 INTO m_col_p2;
  END IF;
  m_has_wl := ('winner_id' = ANY(m_cols)) AND ('loser_id' = ANY(m_cols));

  -- Fecha en matches
  SELECT x FROM unnest(ARRAY['match_date','date','start_time','event_date','start_at','started_at']) x
   WHERE x = ANY(m_cols) LIMIT 1 INTO m_col_date;

  -- Surface
  IF 'surface' = ANY(m_cols) THEN
    m_col_surface := 'surface';
  END IF;

  -- Nombre de torneo directo en matches
  SELECT x FROM unnest(ARRAY['tournament_name','tourney_name','event_name','competition_name']) x
   WHERE x = ANY(m_cols) LIMIT 1 INTO m_col_tname;

  -- Columnas de tournaments (si existe)
  IF has_tourn THEN
    SELECT array_agg(lower(column_name)::text)
    INTO t_cols
    FROM information_schema.columns
    WHERE table_schema='estratego_v1' AND table_name='tournaments';

    t_has_tourney_id := ('tourney_id' = ANY(t_cols));

    -- nombre de torneo en tournaments
    SELECT x FROM unnest(ARRAY['tourney_name','tournament_name','name','event_name','competition_name']) x
     WHERE x = ANY(t_cols) LIMIT 1 INTO t_col_tname;

    -- fecha en tournaments
    SELECT x FROM unnest(ARRAY['tourney_date','tournament_date','date','start_date']) x
     WHERE x = ANY(t_cols) LIMIT 1 INTO t_col_date;

    -- surface en tournaments (si existiera)
    IF 'surface' = ANY(t_cols) THEN
      t_col_surface := 'surface';
    END IF;
  END IF;

  -- FROM / JOIN
  IF has_tourn AND t_has_tourney_id AND 'tourney_id' = ANY(m_cols) THEN
    from_clause := ' FROM estratego_v1.matches m LEFT JOIN estratego_v1.tournaments t ON t.tourney_id = m.tourney_id ';
  ELSE
    from_clause := ' FROM estratego_v1.matches m ';
    t_col_tname := NULL;
    t_col_date  := NULL;
    t_col_surface := NULL;
  END IF;

  -- match_date: preferir matches; si no, tournaments; si tampoco, NULL
  IF m_col_date IS NOT NULL THEN
    date_expr := format('m.%I::date', m_col_date);
  ELSIF t_col_date IS NOT NULL THEN
    -- acepta entero/texto YYYYMMDD
    date_expr := format(
      'CASE WHEN t.%1$I IS NULL THEN NULL::date ELSE to_date(t.%1$I::text, ''YYYYMMDD'') END',
      t_col_date
    );
  ELSE
    date_expr := 'NULL::date';
  END IF;

  -- tournament_name: preferir matches; si no, tournaments; si tampoco, NULL
  IF m_col_tname IS NOT NULL THEN
    tname_expr := format('lower(m.%I::text)', m_col_tname);
  ELSIF t_col_tname IS NOT NULL THEN
    tname_expr := format('lower(t.%I::text)', t_col_tname);
  ELSE
    tname_expr := 'NULL::text';
  END IF;

  -- surface: preferir matches; si no, tournaments; si tampoco, NULL
  IF m_col_surface IS NOT NULL THEN
    surf_expr := format('lower(m.%I::text)', m_col_surface);
  ELSIF t_col_surface IS NOT NULL THEN
    surf_expr := format('lower(t.%I::text)', t_col_surface);
  ELSE
    surf_expr := 'NULL::text';
  END IF;

  -- Comprobaciones mínimas para poder construir filas
  IF NOT m_has_p1p2 AND NOT m_has_wl THEN
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.fs_matches_long AS
      SELECT NULL::date AS match_date, NULL::int AS player_id, NULL::int AS opponent_id,
             NULL::int AS winner_id, NULL::text AS tournament_name, NULL::text AS surface
      WHERE false
    $q$;
    COMMENT ON VIEW public.fs_matches_long IS 'FS: matches sin (p1/p2) ni (winner/loser) — vista vacía.';
    GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
    RETURN;
  END IF;

  -- Crear vista larga (2 filas por partido), conservando el ORDEN de columnas esperado
  EXECUTE '
    CREATE OR REPLACE VIEW public.fs_matches_long AS
    WITH base AS (
      SELECT
        '|| date_expr ||' AS match_date,
        '|| tname_expr ||' AS tournament_name,
        '|| surf_expr  ||' AS surface,
        '||
        CASE
          WHEN m_has_p1p2 THEN format('m.%I::int', m_col_p1)
          ELSE 'm.winner_id::int'
        END ||' AS p_a,
        '||
        CASE
          WHEN m_has_p1p2 THEN format('m.%I::int', m_col_p2)
          ELSE 'm.loser_id::int'
        END ||' AS p_b,
        '||
        CASE
          WHEN m_has_wl THEN 'm.winner_id::int'
          ELSE 'NULL::int'
        END ||' AS winner
      '|| from_clause ||'
    )
    SELECT match_date, p_a AS player_id, p_b AS opponent_id, winner AS winner_id, tournament_name, surface FROM base
    UNION ALL
    SELECT match_date, p_b AS player_id, p_a AS opponent_id, winner AS winner_id, tournament_name, surface FROM base
  ';

  COMMENT ON VIEW public.fs_matches_long IS 'FS: derivada de estratego_v1.matches (2 filas/partido). Usa nombre/fecha/surface del propio matches o, si faltan, de estratego_v1.tournaments.';
  GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
END$$;
