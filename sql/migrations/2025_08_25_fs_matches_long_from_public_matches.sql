-- Requiere: public.players (id uuid, name text) y estratego_v1.players (player_id int, name text)
CREATE EXTENSION IF NOT EXISTS unaccent;

-- 1) Puente UUID -> INT por nombre normalizado
CREATE OR REPLACE VIEW public.players_bridge_uuid_to_int AS
SELECT
  p.id    AS player_uuid,
  p.name  AS name_uuid,
  e.player_id AS player_int,
  e.name  AS name_int
FROM public.players p
JOIN estratego_v1.players e
  ON lower(unaccent(p.name)) = lower(unaccent(e.name));

COMMENT ON VIEW public.players_bridge_uuid_to_int IS 'Mapea public.players(id uuid,name) a estratego_v1.players(player_id int) por nombre normalizado.';
GRANT SELECT ON public.players_bridge_uuid_to_int TO anon, authenticated, service_role;

-- 2) Vista canónica larga (1 fila por jugador y partido) desde public.matches
--    NOTA: si public.matches NO tiene tournament_name, lo dejamos NULL (hist_speed podrá seguir en 0 hasta añadir ese dato).
CREATE OR REPLACE VIEW public.fs_matches_long AS
SELECT
  m.date::date                   AS match_date,
  NULL::text                     AS tournament_name,      -- ← si tu tabla tiene columna, cámbiala por m.tournament_name::text
  lower(m.surface::text)         AS surface,
  b1.player_int                  AS player_id,
  b2.player_int                  AS opponent_id,
  bw.player_int                  AS winner_id
FROM public.matches m
JOIN public.players_bridge_uuid_to_int b1 ON b1.player_uuid = m.player_id
JOIN public.players_bridge_uuid_to_int b2 ON b2.player_uuid = m.opponent_id
JOIN public.players_bridge_uuid_to_int bw ON bw.player_uuid = m.winner_id;

COMMENT ON VIEW public.fs_matches_long IS 'Vista canónica para FS: derivada de public.matches (UUIDs mapeados a INT por nombre).';
GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
