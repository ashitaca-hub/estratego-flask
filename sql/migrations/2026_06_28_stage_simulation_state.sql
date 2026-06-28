-- 2026_06_28_stage_simulation_state.sql
-- "Simular xN" (usado para alimentar las estadisticas de /simulation/.../analytics)
-- pintaba directamente sobre public.draw_matches: cada run resetea top_id/bot_id/
-- winner_id de TODAS las rondas a partir de la primera con entradas reales, lo que
-- borraba cualquier ganador marcado a mano por el usuario en el cuadro visible
-- (y lo dejaba con el resultado aleatorio de la ultima run al terminar el lote).
--
-- El cuadro visible debe reflejar solo decisiones manuales del usuario (o, mas
-- adelante, resultados reales) -- nunca el resultado de una simulacion para stats.
--
-- Fix: las funciones de "Simular xN" pasan a operar sobre una tabla de scratch
-- (simulation_draw_state) en vez de draw_matches. draw_matches solo se LEE (para
-- copiar las posiciones reales de la primera ronda con entradas), nunca se escribe.
--
-- El boton unico "Simular" (api/simulate/route.ts, simulate_prepare_bracket +
-- simulate_one_round) NO se toca: sigue pintando una ronda hipotetica completa
-- sobre el cuadro visible a proposito, con su propio botón "Resetear".

CREATE TABLE IF NOT EXISTS public.simulation_draw_state (
  tourney_id text NOT NULL,
  id text NOT NULL,
  round text NOT NULL,
  top_id integer,
  bot_id integer,
  winner_id integer,
  PRIMARY KEY (tourney_id, id)
);

CREATE OR REPLACE FUNCTION public.simulate_stage_prepare(p_tourney_id text)
RETURNS text[]
LANGUAGE plpgsql
AS $function$
DECLARE
  rounds          CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F'];
  first_round_idx INT;
  first_round     TEXT;
  matches_prev    INT;
  round_idx       INT;
  gen_match_num   INT;
BEGIN
  SELECT idx, rounds[idx]
  INTO first_round_idx, first_round
  FROM (
    SELECT i AS idx
    FROM generate_subscripts(rounds, 1) AS g(i)
    ORDER BY i
  ) t
  WHERE EXISTS (
    SELECT 1
    FROM public.draw_matches
    WHERE tourney_id = p_tourney_id
      AND round      = rounds[t.idx]
  )
  ORDER BY idx
  LIMIT 1;

  IF first_round_idx IS NULL THEN
    RETURN ARRAY[]::TEXT[];
  END IF;

  DELETE FROM public.simulation_draw_state WHERE tourney_id = p_tourney_id;

  INSERT INTO public.simulation_draw_state (tourney_id, id, round, top_id, bot_id, winner_id)
  SELECT tourney_id, id, round, top_id, bot_id, NULL
  FROM public.draw_matches
  WHERE tourney_id = p_tourney_id
    AND round      = first_round;

  SELECT COUNT(*)
    INTO matches_prev
    FROM public.simulation_draw_state
   WHERE tourney_id = p_tourney_id
     AND round      = first_round;

  IF matches_prev > 0 THEN
    FOR round_idx IN first_round_idx + 1 .. array_length(rounds, 1) LOOP
      matches_prev := (matches_prev + 1) / 2;
      EXIT WHEN matches_prev <= 0;

      FOR gen_match_num IN 1 .. matches_prev LOOP
        INSERT INTO public.simulation_draw_state (tourney_id, id, round, top_id, bot_id, winner_id)
        VALUES (
          p_tourney_id,
          CONCAT(rounds[round_idx], '-', gen_match_num),
          rounds[round_idx],
          NULL, NULL, NULL
        )
        ON CONFLICT (tourney_id, id) DO NOTHING;
      END LOOP;
    END LOOP;
  END IF;

  RETURN rounds[first_round_idx:array_length(rounds, 1)];
END;
$function$;

CREATE OR REPLACE FUNCTION public.simulate_stage_round(p_tourney_id text, p_round text)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  rounds      CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F'];
  round_idx   INT;
  next_round  TEXT;
  match_year  INT;
BEGIN
  SELECT i INTO round_idx FROM generate_subscripts(rounds, 1) AS g(i) WHERE rounds[i] = p_round;
  next_round := CASE WHEN round_idx < array_length(rounds, 1) THEN rounds[round_idx + 1] ELSE NULL END;

  SELECT LEFT(tourney_date::text, 4)::INT
  INTO match_year
  FROM estratego_v1.tournaments
  WHERE tourney_id = p_tourney_id
  LIMIT 1;

  UPDATE public.simulation_draw_state
  SET winner_id = COALESCE(top_id, bot_id)
  WHERE tourney_id = p_tourney_id
    AND round      = p_round
    AND (top_id IS NULL) <> (bot_id IS NULL);

  WITH pairs AS (
    SELECT DISTINCT LEAST(top_id, bot_id) AS lo, GREATEST(top_id, bot_id) AS hi
    FROM public.simulation_draw_state
    WHERE tourney_id = p_tourney_id
      AND round      = p_round
      AND top_id IS NOT NULL
      AND bot_id IS NOT NULL
  ),
  probs AS (
    SELECT lo, hi,
           (get_extended_prematch_summary(p_tourney_id, match_year, lo, hi) -> 'playerA' ->> 'win_probability')::FLOAT AS prob_lo
    FROM pairs
  )
  UPDATE public.simulation_draw_state s
  SET winner_id = CASE
    WHEN random() < (CASE WHEN s.top_id = pr.lo THEN pr.prob_lo ELSE 1 - pr.prob_lo END)
      THEN s.top_id
      ELSE s.bot_id
    END
  FROM probs pr
  WHERE s.tourney_id = p_tourney_id
    AND s.round      = p_round
    AND s.top_id IS NOT NULL
    AND s.bot_id IS NOT NULL
    AND pr.lo = LEAST(s.top_id, s.bot_id)
    AND pr.hi = GREATEST(s.top_id, s.bot_id);

  IF next_round IS NOT NULL THEN
    WITH advances AS (
      SELECT
        next_round || '-' || ((split_part(id, '-', 2)::INT + 1) / 2) AS next_id,
        MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 1 THEN winner_id END) AS new_top,
        MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 0 THEN winner_id END) AS new_bot
      FROM public.simulation_draw_state
      WHERE tourney_id = p_tourney_id
        AND round      = p_round
        AND winner_id IS NOT NULL
      GROUP BY next_round || '-' || ((split_part(id, '-', 2)::INT + 1) / 2)
    )
    UPDATE public.simulation_draw_state nm
    SET top_id = COALESCE(a.new_top, nm.top_id),
        bot_id = COALESCE(a.new_bot, nm.bot_id)
    FROM advances a
    WHERE nm.tourney_id = p_tourney_id
      AND nm.round      = next_round
      AND nm.id         = a.next_id;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.simulate_stage_record_run_result(p_tourney_id text, p_run_number integer)
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
    FROM public.simulation_draw_state dm
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
