-- 2026_06_27_track_simulation_champion.sql
-- simulate_record_run_result guardaba reached_round = 'F' tanto para el
-- finalista perdedor como para el campeon (ganar la final no generaba
-- ninguna ronda "siguiente" en la que aterrizar, asi que ambos colapsaban
-- al mismo valor). Esto impide mostrar una columna de probabilidad de
-- campeon en la tabla de resultados, distinta de "llego a la final".
--
-- Fix: se anade una etapa virtual 'W' (un escalon mas alla de 'F') solo
-- para quien gana el partido de la final. No representa una ronda real
-- de draw_matches, es solo una etiqueta de agregacion.
--
-- Nota: las runs ya guardadas antes de este cambio seguiran mostrando
-- 'F' para el campeon de esas runs (no se puede reconstruir a posteriori
-- sin volver a simular). Conviene re-lanzar las simulaciones de prueba
-- recientes (Hamburgo, Wimbledon) tras aplicar esto.

CREATE OR REPLACE FUNCTION public.simulate_record_run_result(p_tourney_id text, p_run_number integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  round_labels CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F','W'];
BEGIN
  WITH round_map AS (
    SELECT round, ord::INT
    FROM unnest(round_labels) WITH ORDINALITY AS t(round, ord)
  ),
  player_stages AS (
    SELECT
      player_id,
      CASE
        WHEN is_winner THEN COALESCE(next_round.ord, current_round.ord + 1)
        ELSE current_round.ord
      END AS stage_ord
    FROM public.draw_matches dm
    CROSS JOIN LATERAL (
      VALUES
        (dm.top_id, dm.winner_id = dm.top_id),
        (dm.bot_id, dm.winner_id = dm.bot_id)
    ) AS p(player_id, is_winner)
    JOIN round_map current_round ON current_round.round = dm.round
    LEFT JOIN round_map next_round ON next_round.ord = current_round.ord + 1
    WHERE dm.tourney_id = p_tourney_id
      AND p.player_id IS NOT NULL
  ),
  best_stage AS (
    SELECT player_id, MAX(stage_ord) AS max_ord
    FROM player_stages
    GROUP BY player_id
  )
  INSERT INTO public.simulation_results (tourney_id, run_number, player_id, reached_round)
  SELECT
    p_tourney_id,
    p_run_number,
    bs.player_id,
    rm.round
  FROM best_stage bs
  JOIN round_map rm ON rm.ord = bs.max_ord
  ON CONFLICT (tourney_id, run_number, player_id)
  DO UPDATE SET reached_round = EXCLUDED.reached_round;
END;
$function$;
