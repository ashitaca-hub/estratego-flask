-- Bootstrap v3: construye public.fs_matches_long buscando tablas candidatas
-- Compatibles:
--  - Formato LARGO:  player_id, opponent_id, winner_id (+ fecha + torneo + surface)
--  - Formato ANCHO:  player1_id, player2_id, winner_id (+ fecha + torneo + surface) → se desdobla a largo

DO $$
DECLARE
  -- Esquemas a inspeccionar (ajusta si tienes más)
  schemas text[] := ARRAY['estratego_v1','public'];

  rec record;
  cols text[];

  -- candidatos por campo
  c_date   text[] := ARRAY['match_date','start_time','event_date','start_at','date','startdate','started_at'];
  c_tourn  text[] := ARRAY['tournament_name','competition_name','event_name','tournament'];
  c_surf   text[] := ARRAY['surface','court_surface','court','surface_name'];

  -- flags por tabla
  has_long boolean;
  has_wide boolean;

  -- columnas detectadas por tabla
  col_date text; col_tour text; col_surf text;
  col_p1   text; col_p2   text; col_win  text;

  part_sql text;
  union_sql text := '';
BEGIN
  -- Recorremos todas las tablas de los esquemas listados
  FOR rec IN
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = ANY(schemas) AND table_type='BASE TABLE'
    ORDER BY table_schema, table_name
  LOOP
    -- columnas en minúsculas para la detección
    SELECT array_agg(lower(column_name)::text)
    INTO cols
    FROM information_schema.columns
    WHERE table_schema=rec.table_schema AND table_name=rec.table_name;

    -- detectar formato largo/ancho
    has_long := ('player_id'=ANY(cols) AND 'opponent_id'=ANY(cols));
    has_wide := (('player1_id'=ANY(cols) OR 'player_1_id'=ANY(cols) OR 'home_player_id'=ANY(cols) OR 'player_a_id'=ANY(cols) OR 'p1_id'=ANY(cols))
             AND ('player2_id'=ANY(cols) OR 'player_2_id'=ANY(cols) OR 'away_player_id'=ANY(cols) OR 'player_b_id'=ANY(cols) OR 'p2_id'=ANY(cols)));

    -- winner_id (obligatoria)
    SELECT x FROM unnest(ARRAY['winner_id','winner_player_id','winner','winnerid']) x WHERE x = ANY(cols) LIMIT 1 INTO col_win;
    IF col_win IS NULL THEN
      CONTINUE; -- sin ganador no podemos calcular winrate → saltamos
    END IF;

    -- fecha (obligatoria para ventana temporal)
    SELECT x FROM unnest(c_date) x WHERE x = ANY(cols) LIMIT 1 INTO col_date;
    IF col_date IS NULL THEN
      CONTINUE; -- sin fecha no sirve para histórico → saltamos
    END IF;

    -- opcionales: torneo/surface
    SELECT x FROM unnest(c_tourn) x WHERE x = ANY(cols) LIMIT 1 INTO col_tour;
    SELECT x FROM unnest(c_surf)  x WHERE x = ANY(cols) LIMIT 1 INTO col_surf;

    IF has_long THEN
      -- LARGO: player_id/opponent_id ya existen
      part_sql := format(
        'SELECT %1$s::date AS match_date,
                %2$s::int  AS player_id,
                %3$s::int  AS opponent_id,
                %4$s::int  AS winner_id,
                %5$s       AS tournament_name,
                %6$s       AS surface
         FROM %7$I.%8$I',
        format('%1$I', col_date),
        'player_id',
        'opponent_id',
        col_win,
        COALESCE(format('%1$I::text', col_tour), 'NULL::text'),
        COALESCE(format('lower(%1$I::text)', col_surf), 'NULL::text'),
        rec.table_schema, rec.table_name
      );

    ELSIF has_wide THEN
      -- ANCHO: necesitamos p1/p2
      SELECT x FROM unnest(ARRAY['player1_id','player_1_id','home_player_id','player_a_id','p1_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p1;
      SELECT x FROM unnest(ARRAY['player2_id','player_2_id','away_player_id','player_b_id','p2_id']) x WHERE x = ANY(cols) LIMIT 1 INTO col_p2;
      IF col_p1 IS NULL OR col_p2 IS NULL THEN
        CONTINUE;
      END IF;

      part_sql := format(
        $fmt$
        WITH base AS (
          SELECT %1$I::date AS match_date,
                 %2$s       AS tournament_name,
                 %3$s       AS surface,
                 %4$I::int  AS p1,
                 %5$I::int  AS p2,
                 %6$I::int  AS winner_id
          FROM %7$I.%8$I
        )
        SELECT match_date, p1 AS player_id, p2 AS opponent_id, winner_id, tournament_name, surface FROM base
        UNION ALL
        SELECT match_date, p2 AS player_id, p1 AS opponent_id, winner_id, tournament_name, surface FROM base
        $fmt$,
        col_date,
        COALESCE(format('%1$I::text', col_tour), 'NULL::text'),
        COALESCE(format('lower(%1$I::text)', col_surf), 'NULL::text'),
        col_p1, col_p2, col_win,
        rec.table_schema, rec.table_name
      );

    ELSE
      CONTINUE; -- no tiene ni formato largo ni ancho → saltamos
    END IF;

    -- ir acumulando las partes
    IF union_sql = '' THEN
      union_sql := part_sql;
    ELSE
      union_sql := union_sql || ' UNION ALL ' || part_sql;
    END IF;
  END LOOP;

  IF union_sql = '' THEN
    -- sin fuentes válidas → vista vacía (placeholder)
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
