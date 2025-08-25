-- Rehacer public.fs_matches_long para que tenga 2 filas por partido (formato largo)
-- Mantiene el orden de columnas existente:
-- (match_date, player_id, opponent_id, winner_id, tournament_name, surface)

CREATE OR REPLACE VIEW public.fs_matches_long AS
SELECT
  m.date::date           AS match_date,
  b1.player_int          AS player_id,
  b2.player_int          AS opponent_id,
  bw.player_int          AS winner_id,
  NULL::text             AS tournament_name,  -- si existe en public.matches, luego lo sustituimos
  lower(m.surface::text) AS surface
FROM public.matches m
JOIN public.players_bridge_uuid_to_int b1 ON b1.player_uuid = m.player_id
JOIN public.players_bridge_uuid_to_int b2 ON b2.player_uuid = m.opponent_id
JOIN public.players_bridge_uuid_to_int bw ON bw.player_uuid = m.winner_id

UNION ALL

SELECT
  m.date::date           AS match_date,
  b2.player_int          AS player_id,        -- ← invertido
  b1.player_int          AS opponent_id,      -- ← invertido
  bw.player_int          AS winner_id,
  NULL::text             AS tournament_name,
  lower(m.surface::text) AS surface
FROM public.matches m
JOIN public.players_bridge_uuid_to_int b1 ON b1.player_uuid = m.player_id
JOIN public.players_bridge_uuid_to_int b2 ON b2.player_uuid = m.opponent_id
JOIN public.players_bridge_uuid_to_int bw ON bw.player_uuid = m.winner_id;

COMMENT ON VIEW public.fs_matches_long IS
'Vista canónica FS en formato largo: 2 filas por partido (UUID→INT por nombre desde public.matches).';
GRANT SELECT ON public.fs_matches_long TO anon, authenticated, service_role;
