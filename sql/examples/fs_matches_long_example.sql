-- Plantilla para crear la vista canónica usada por las RPCs del FS.
-- ADAPTA los nombres marcados como TODO a tu tabla real de partidos.

-- Ejemplo partiendo de una tabla "estratego_v1.matches" con columnas:
--   id, player1_id, player2_id, winner_id, start_time, tournament_name, surface
-- Si tu tabla ya está en formato "long" (una fila por jugador), comenta la parte UNNEST
-- y selecciona directamente player_id/opponent_id/winner_id.

CREATE OR REPLACE VIEW public.fs_matches_long AS
WITH base AS (
  SELECT
    m.id,
    m.start_time::date       AS match_date,             -- TODO: fecha del partido
    lower(m.surface)         AS surface,                -- TODO: si no hay columna surface, déjalo NULL y el filtro surface no devolverá nada
    m.tournament_name        AS tournament_name,        -- TODO: si no hay, usa competición o evento
    m.player1_id             AS p1,
    m.player2_id             AS p2,
    m.winner_id              AS winner_id               -- ganador (igual a p1 o p2)
  FROM estratego_v1.matches m                            -- TODO: cambia al nombre real de tu tabla
),
long AS (
  SELECT
    match_date,
    tournament_name,
    surface,
    p1 AS player_id,
    p2 AS opponent_id,
    winner_id
  FROM base
  UNION ALL
  SELECT
    match_date,
    tournament_name,
    surface,
    p2 AS player_id,
    p1 AS opponent_id,
    winner_id
  FROM base
)
SELECT * FROM long;

GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica: 1 fila por jugador y partido, usada por las RPCs del FS.';
