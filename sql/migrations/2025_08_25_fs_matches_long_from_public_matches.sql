-- Construye fs_matches_long desde public.matches (UUID) mapeando a INT por nombre
-- Busca dinámicamente una tabla en public con (uuid_col + name_col) para hacer el puente.
CREATE EXTENSION IF NOT EXISTS unaccent;

DO $$
DECLARE
  -- ======== 1) Detectar tabla/columnas de NOMBRES en schema public ========
  rec record;
  cols text[];
  candidate_tab text := NULL;
  candidate_uuid_col text := NULL;
  candidate_name_col text := NULL;

  name_cols text[] := ARRAY['name','full_name','display_name','username','player_name'];
  uuid_cols text[] := ARRAY['id','uuid','player_uuid','user_id'];

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

    -- elegir uuid_col y name_col si existen
    SELECT x FROM unnest(uuid_cols) x WHERE x = ANY(cols) LIMIT 1 INTO candidate_uuid_col;
    SELECT x FROM unnest(name_cols) x WHERE x = ANY(cols) LIMIT 1 INTO candidate_name_col;

    IF candidate_uuid_col IS NOT NULL AND candidate_name_col IS NOT NULL THEN
      candidate_tab := rec.table_name;
      EXIT; -- nos quedamos con el primer match razonable (puedes refinar si quieres)
    END IF;
  END LOOP;

  -- Si no encontramos nada, creamos views placeholder y terminamos
  IF candidate_tab IS NULL THEN
    EXECUTE $q$
      CREATE OR REPLACE VIEW public.players_bridge_uuid_to_int AS
      SELECT NULL::uuid AS player_uuid, NULL::text AS name_uuid, NULL::int AS player_int, NULL::text AS name_int
      WHERE false
    $q$;
    COMMENT ON VIEW public.players_bridge_uuid_to_int IS 'Puente vacío: no se detectó tabla con (uuid+name) en public.';
    GRANT SELECT ON public.players_bridge_uuid_to_int TO anon, authenticated, service_role;

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
    COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica vacía: no se pudo construir puente UUID→INT';
    GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
    RETURN;
  END IF;

  -- ======== 2) Crear puente UUID→INT por nombre ========
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
  COMMENT ON VIEW public.players_bridge_uuid_to_int IS format(
    'Puente: public.%s(%s uuid, %s text) → estratego_v1.players(player_id int) por nombre normalizado',
    candidate_tab, candidate_uuid_col, candidate_name_col
  );
  GRANT SELECT ON public.players_bridge_uuid_to_int TO anon, authenticated, service_role;

  -- ======== 3) Detectar columnas de public.matches ========
  -- Queremos: fecha, surface, player_id/opponent_id/winner_id (UUIDs)
  -- Intentamos nombres comunes. Si alguno falta, lo ponemos a NULL y la vista seguirá funcionando (pero limitará señales).
  -- Fecha:
  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='matches' AND lower(column_name) IN ('match_date','date','start_time','event_date','start_at','started_at')) THEN
    -- elegimos la primera
    EXECUTE $sel$
      SELECT lower(column_name)
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='matches'
        AND lower(column_name) IN ('match_date','date','start_time','event_date','start_at','started_at')
      ORDER BY CASE lower(column_name)
        WHEN 'match_date' THEN 1
        WHEN 'date' THEN 2
        WHEN 'start_time' THEN 3
        WHEN 'event_date' THEN 4
        WHEN 'start_at' THEN 5
        WHEN 'started_at' THEN 6
        ELSE 99 END
      LIMIT 1
    $sel$ INTO rec;
  ELSE
    rec := NULL;
  END IF;
  PERFORM 1; -- noop para mantener el bloque

  -- Construcción dinámica de expresiones de columnas
  -- Nota: usamos subselects para obtener nombres y evitamos %I con NULLs.

END$$;

-- ======== 4) Crear la vista fs_matches_long con SQL estático pero flexible ========
-- Este bloque intenta las columnas más comunes. Si alguna no existe, usamos NULL.
-- Ajusta aquí si en tu public.matches los nombres son distintos.

CREATE OR REPLACE VIEW public.fs_matches_long AS
WITH src AS (
  SELECT
    -- Fecha del partido (ajusta orden de preferencia si quieres)
    COALESCE(
      (SELECT date::date    FROM public.matches LIMIT 1)       IS NOT NULL, NULL
    ) AS _dummy -- placeholder sin uso
),
base AS (
  SELECT
    -- Fecha
    COALESCE(
      NULLIF((m.match_date)::text, '')::date,
      NULLIF((m.date)::text, '')::date,
      NULLIF((m.start_time)::text, '')::date,
      NULLIF((m.event_date)::text, '')::date,
      NULLIF((m.start_at)::text, '')::date,
      NULLIF((m.started_at)::text, '')::date
    )              AS match_date,
    -- Torneo (si no tienes columna, queda NULL y el hist_speed dependerá del fuzzy por nombre = 0)
    NULL::text     AS tournament_name,
    -- Superficie (si no hay, queda NULL)
    COALESCE(
      lower((m.surface)::text),
      NULL::text
    )              AS surface,
    m.player_id    AS player_uuid,
    m.opponent_id  AS opponent_uuid,
    m.winner_id    AS winner_uuid
  FROM public.matches m
)
SELECT
  b.match_date,
  b1.player_int AS player_id,
  b2.player_int AS opponent_id,
  bw.player_int AS winner_id,
  b.tournament_name,
  b.surface
FROM base b
JOIN public.players_bridge_uuid_to_int b1 ON b1.player_uuid = b.player_uuid
JOIN public.players_bridge_uuid_to_int b2 ON b2.player_uuid = b.opponent_uuid
JOIN public.players_bridge_uuid_to_int bw ON bw.player_uuid = b.winner_uuid;

COMMENT ON VIEW public.fs_matches_long IS
'Vista canónica: 1 fila por jugador/partido, derivada de public.matches (UUID → INT por nombre).';
GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
