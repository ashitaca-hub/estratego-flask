-- Construye fs_matches_long desde public.matches (UUID) mapeando a INT por nombre
CREATE EXTENSION IF NOT EXISTS unaccent;

DO $pl$
DECLARE
  rec record;
  cols text[];
  candidate_tab text := NULL;
  candidate_uuid_col text := NULL;
  candidate_name_col text := NULL;

  name_cols text[] := ARRAY['name','full_name','display_name','username','player_name'];
  uuid_cols text[] := ARRAY['id','uuid','player_uuid','user_id'];
  cmt text;
BEGIN
  FOR rec IN
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_type='BASE TABLE'
    ORDER BY table_name
  LOOP
    SELECT array_agg(lower(column_name)::text)
      INTO cols
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name=rec.table_name;

    IF cols IS NULL THEN CONTINUE; END IF;

    SELECT x FROM unnest(uuid_cols) x WHERE x = ANY(cols) LIMIT 1 INTO candidate_uuid_col;
    SELECT x FROM unnest(name_cols) x WHERE x = ANY(cols) LIMIT 1 INTO candidate_name_col;

    IF candidate_uuid_col IS NOT NULL AND candidate_name_col IS NOT NULL THEN
      candidate_tab := rec.table_name;
      EXIT;
    END IF;
  END LOOP;

  IF candidate_tab IS NULL THEN
    EXECUTE '
      CREATE OR REPLACE VIEW public.players_bridge_uuid_to_int AS
      SELECT NULL::uuid AS player_uuid, NULL::text AS name_uuid, NULL::int AS player_int, NULL::text AS name_int
      WHERE false
    ';
    EXECUTE 'COMMENT ON VIEW public.players_bridge_uuid_to_int IS '
            || quote_literal('Puente vacío: no se detectó tabla con (uuid+name) en public.');
  ELSE
    EXECUTE format(
      'CREATE OR REPLACE VIEW public.players_bridge_uuid_to_int AS
       SELECT
         p.%1$I::uuid AS player_uuid,
         p.%2$I::text AS name_uuid,
         e.player_id  AS player_int,
         e.name       AS name_int
       FROM public.%3$I p
       JOIN estratego_v1.players e
         ON lower(unaccent(p.%2$I::text)) = lower(unaccent(e.name::text));',
      candidate_uuid_col, candidate_name_col, candidate_tab
    );
    cmt := format(
      'Puente: public.%s(%s uuid, %s text) → estratego_v1.players(player_id int) por nombre normalizado',
      candidate_tab, candidate_uuid_col, candidate_name_col
    );
    EXECUTE format('COMMENT ON VIEW public.players_bridge_uuid_to_int IS %L;', cmt);
  END IF;

  EXECUTE 'GRANT SELECT ON public.players_bridge_uuid_to_int TO anon, authenticated, service_role;';
END $pl$;

-- IMPORTANTÍSIMO: mantener el ORDEN/NOMBRE de columnas ya existente:
-- (match_date, player_id, opponent_id, winner_id, tournament_name, surface)
CREATE OR REPLACE VIEW public.fs_matches_long AS
SELECT
  m.date::date                   AS match_date,
  b1.player_int                  AS player_id,
  b2.player_int                  AS opponent_id,
  bw.player_int                  AS winner_id,
  NULL::text                     AS tournament_name,      -- si tienes columna en matches, cámbiala aquí por m.tournament_name::text
  lower(m.surface::text)         AS surface
FROM public.matches m
JOIN public.players_bridge_uuid_to_int b1 ON b1.player_uuid = m.player_id
JOIN public.players_bridge_uuid_to_int b2 ON b2.player_uuid = m.opponent_id
JOIN public.players_bridge_uuid_to_int bw ON bw.player_uuid = m.winner_id;

COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica para FS (1 fila por jugador/partido) desde public.matches (UUID→INT por nombre).';
GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
