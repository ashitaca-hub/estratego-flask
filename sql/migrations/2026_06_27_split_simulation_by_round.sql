-- 2026_06_27_split_simulation_by_round.sql
-- Resuelve el problema pendiente de la sesion anterior: un draw de 128
-- (Wimbledon) no cabe en una sola llamada porque el statement_timeout de
-- 8s del rol authenticator de PostgREST se queda corto (una sola pasada
-- completa son ~127 partidos x ~125ms ~= 16s, y el set_config interno no
-- puede extender retroactivamente el timeout de una llamada ya en curso).
--
-- Solucion elegida: partir la simulacion en una llamada por ronda en vez
-- de una llamada para todo el torneo. Cada llamada queda muy por debajo
-- de 8s salvo, en el peor de los casos, la propia ronda R128 de un draw
-- de 128 (64 parejas x ~125ms ~= 8s, al limite). Si eso resulta ser un
-- problema en la practica, el siguiente paso seria sub-dividir tambien
-- esa ronda por lotes de parejas, pero no se aborda en esta migracion.
--
-- Funciones nuevas (sustituyen a simulate_full_tournament y a la logica
-- interna de simulate_multiple_runs, que se eliminan):
--   simulate_prepare_bracket(p_tourney_id)      -> text[] rondas a procesar
--   simulate_one_round(p_tourney_id, p_round)   -> procesa una ronda
--   simulate_record_run_result(p_tourney_id, p_run_number)
--   simulate_reset_results(p_tourney_id)
--   simulate_next_run_number(p_tourney_id)      -> int
--
-- La orquestacion (llamar a estas funciones en el orden correcto, una
-- vez por ronda y, para las estadisticas, una vez por run) vive ahora en
-- las rutas API de Next.js (app/api/simulate/route.ts y
-- app/api/simulate/multiple/route.ts), no en una unica funcion SQL.

DROP FUNCTION IF EXISTS public.simulate_full_tournament(text);
DROP FUNCTION IF EXISTS public.simulate_multiple_runs(text, integer, integer, boolean);

CREATE OR REPLACE FUNCTION public.simulate_prepare_bracket(p_tourney_id text)
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
    RAISE NOTICE 'No hay rondas en draw_matches para %', p_tourney_id;
    RETURN ARRAY[]::TEXT[];
  END IF;

  SELECT COUNT(*)
    INTO matches_prev
    FROM public.draw_matches
   WHERE tourney_id = p_tourney_id
     AND round      = first_round;

  IF matches_prev > 0 THEN
    FOR round_idx IN first_round_idx + 1 .. array_length(rounds, 1) LOOP
      matches_prev := (matches_prev + 1) / 2;
      EXIT WHEN matches_prev <= 0;

      FOR gen_match_num IN 1 .. matches_prev LOOP
        INSERT INTO public.draw_matches (id, tourney_id, round, top_id, bot_id, winner_id)
        VALUES (
          CONCAT(rounds[round_idx], '-', gen_match_num),
          p_tourney_id,
          rounds[round_idx],
          NULL, NULL, NULL
        )
        ON CONFLICT (tourney_id, id) DO NOTHING;
      END LOOP;
    END LOOP;
  END IF;

  CREATE TEMP TABLE tmp_first_round_snapshot ON COMMIT DROP AS
  SELECT id, top_id, bot_id
  FROM public.draw_matches
  WHERE tourney_id = p_tourney_id
    AND round      = first_round;

  UPDATE public.draw_matches
  SET top_id = NULL, bot_id = NULL, winner_id = NULL
  WHERE tourney_id = p_tourney_id;

  UPDATE public.draw_matches dm
  SET top_id = tmp.top_id, bot_id = tmp.bot_id
  FROM tmp_first_round_snapshot tmp
  WHERE dm.tourney_id = p_tourney_id
    AND dm.id        = tmp.id;

  RETURN rounds[first_round_idx:array_length(rounds, 1)];
END;
$function$;

CREATE OR REPLACE FUNCTION public.simulate_one_round(p_tourney_id text, p_round text)
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

  -- Byes: solo un lado presente -> avanza directo, sin necesidad de probabilidad.
  UPDATE public.draw_matches
  SET winner_id = COALESCE(top_id, bot_id)
  WHERE tourney_id = p_tourney_id
    AND round      = p_round
    AND (top_id IS NULL) <> (bot_id IS NULL);

  WITH pairs AS (
    SELECT DISTINCT LEAST(top_id, bot_id) AS lo, GREATEST(top_id, bot_id) AS hi
    FROM public.draw_matches
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
  UPDATE public.draw_matches dm
  SET winner_id = CASE
    WHEN random() < (CASE WHEN dm.top_id = pr.lo THEN pr.prob_lo ELSE 1 - pr.prob_lo END)
      THEN dm.top_id
      ELSE dm.bot_id
    END
  FROM probs pr
  WHERE dm.tourney_id = p_tourney_id
    AND dm.round      = p_round
    AND dm.top_id IS NOT NULL
    AND dm.bot_id IS NOT NULL
    AND pr.lo = LEAST(dm.top_id, dm.bot_id)
    AND pr.hi = GREATEST(dm.top_id, dm.bot_id);

  IF next_round IS NOT NULL THEN
    WITH advances AS (
      SELECT
        next_round || '-' || ((split_part(id, '-', 2)::INT + 1) / 2) AS next_id,
        MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 1 THEN winner_id END) AS new_top,
        MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 0 THEN winner_id END) AS new_bot
      FROM public.draw_matches
      WHERE tourney_id = p_tourney_id
        AND round      = p_round
        AND winner_id IS NOT NULL
      GROUP BY next_round || '-' || ((split_part(id, '-', 2)::INT + 1) / 2)
    )
    UPDATE public.draw_matches nm
    SET top_id = COALESCE(a.new_top, nm.top_id),
        bot_id = COALESCE(a.new_bot, nm.bot_id)
    FROM advances a
    WHERE nm.tourney_id = p_tourney_id
      AND nm.round      = next_round
      AND nm.id         = a.next_id;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION public.simulate_record_run_result(p_tourney_id text, p_run_number integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
DECLARE
  round_labels CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F'];
BEGIN
  WITH round_map AS (
    SELECT round, ord::INT
    FROM unnest(round_labels) WITH ORDINALITY AS t(round, ord)
  ),
  player_stages AS (
    SELECT
      player_id,
      CASE
        WHEN is_winner THEN COALESCE(next_round.ord, current_round.ord)
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

CREATE OR REPLACE FUNCTION public.simulate_reset_results(p_tourney_id text)
RETURNS void
LANGUAGE sql
AS $function$
  DELETE FROM public.simulation_results WHERE tourney_id = p_tourney_id;
$function$;

CREATE OR REPLACE FUNCTION public.simulate_next_run_number(p_tourney_id text)
RETURNS integer
LANGUAGE sql
AS $function$
  SELECT COALESCE(MAX(run_number), 0) + 1
  FROM public.simulation_results
  WHERE tourney_id = p_tourney_id;
$function$;
