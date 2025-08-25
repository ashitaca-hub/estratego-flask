-- Crea/repone la vista canónica public.fs_matches_long
-- Intenta detectar una tabla/vista fuente. Si no encuentra ninguna, crea una vista vacía con el esquema correcto.

DO $$
DECLARE
  src text := NULL;
BEGIN
  -- Candidatos (ajusta si tu tabla real se llama distinto)
  IF to_regclass('estratego_v1.matches_long') IS NOT NULL THEN
    src := 'SELECT match_date::date, player_id::int, opponent_id::int, winner_id::int, tournament_name::text, lower(surface)::text AS surface FROM estratego_v1.matches_long';
  ELSIF to_regclass('estratego_v1.matches') IS NOT NULL THEN
    -- Supuesto ancho: una fila por partido con p1/p2 y winner_id
    src := $q$
      WITH base AS (
        SELECT
          m.start_time::date AS match_date,
          m.tournament_name::text AS tournament_name,
          lower(m.surface)::text AS surface,
          m.player1_id::int AS p1,
          m.player2_id::int AS p2,
          m.winner_id::int  AS winner_id
        FROM estratego_v1.matches m
      ),
      long AS (
        SELECT match_date, tournament_name, surface, p1 AS player_id, p2 AS opponent_id, winner_id FROM base
        UNION ALL
        SELECT match_date, tournament_name, surface, p2 AS player_id, p1 AS opponent_id, winner_id FROM base
      )
      SELECT match_date, player_id, opponent_id, winner_id, tournament_name, surface FROM long
    $q$;
  ELSIF to_regclass('public.matches_long') IS NOT NULL THEN
    src := 'SELECT match_date::date, player_id::int, opponent_id::int, winner_id::int, tournament_name::text, lower(surface)::text AS surface FROM public.matches_long';
  END IF;

  -- Dropear y crear
  IF src IS NOT NULL THEN
    EXECUTE 'CREATE OR REPLACE VIEW public.fs_matches_long AS ' || src;
  ELSE
    -- Vista vacía (placeholder) para no romper migraciones; devuelves 0s hasta que la repongas.
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
  END IF;

  COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica (1 fila por jugador y partido) usada por el Feature Store.';
  GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
END$$;
