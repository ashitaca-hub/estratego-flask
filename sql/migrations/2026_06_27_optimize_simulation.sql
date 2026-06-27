-- 2026_06_27_optimize_simulation.sql
-- Optimiza la simulacion de torneos (simulate_full_tournament /
-- simulate_multiple_runs), que sufria un cuello de botella severo:
-- get_extended_prematch_summary se llamaba una vez por partido, de forma
-- secuencial, y rankings_snapshot/rankings_snapshot_v2 no tenian ningun
-- indice usable por player_id (full scan de 78k/7.8k filas en cada
-- llamada, dos veces por llamada). Para un draw de 128, una sola
-- simulacion completa (127 partidos) superaba con holgura el
-- statement_timeout de 8s del rol authenticator de PostgREST.
--
-- Cambios:
--   1) Indices que faltaban en rankings_snapshot(player_id) y
--      rankings_snapshot_v2(player_id) -- beneficia tambien al dialogo
--      de prematch normal, no solo a la simulacion.
--   2) simulate_full_tournament reescrita para procesar cada ronda en
--      bloque (una consulta por paso) en vez de partido a partido, y
--      cachear la probabilidad por pareja de jugador dentro de la misma
--      transaccion (get_extended_prematch_summary es simetrica, sum=1,
--      verificado).
--   3) Codigo muerto eliminado: simulate_next_round (cara/cruz simple,
--      no se llamaba desde ningun sitio) y la version de 3 argumentos
--      de simulate_multiple_runs (sin p_reset, superada por la de 4).
--
-- Resultado medido: get_extended_prematch_summary paso de >150ms a
-- ~125ms reales por llamada (EXPLAIN ANALYZE). Sigue siendo demasiado
-- para que un draw de 128 quepa en una sola llamada (127 partidos x
-- ~125ms ~= 16s), pero los draws normales (28-64, la mayoria de los
-- torneos) ya simulan varias runs por llamada de forma fiable.

CREATE INDEX IF NOT EXISTS idx_rankings_snapshot_player_id
  ON estratego_v1.rankings_snapshot (player_id);

CREATE INDEX IF NOT EXISTS idx_rankings_snapshot_v2_player_id
  ON estratego_v1.rankings_snapshot_v2 (player_id);

ANALYZE estratego_v1.rankings_snapshot;
ANALYZE estratego_v1.rankings_snapshot_v2;

CREATE OR REPLACE FUNCTION public.simulate_full_tournament(p_tourney_id text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  rounds          CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F'];
  first_round_idx INT;
  first_round     TEXT;
  first_round_matches INT;
  matches_prev    INT;
  gen_match_num   INT;
  round_idx       INT;
  current_round   TEXT;
  next_round      TEXT;
  match_year      INT;
BEGIN
  PERFORM set_config('statement_timeout', '300000', true);

  CREATE TEMP TABLE IF NOT EXISTS sim_prob_cache (
    player_lo INT,
    player_hi INT,
    prob_lo   DOUBLE PRECISION,
    PRIMARY KEY (player_lo, player_hi)
  ) ON COMMIT DROP;

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
    RETURN;
  END IF;

  SELECT COUNT(*)
    INTO first_round_matches
    FROM public.draw_matches
   WHERE tourney_id = p_tourney_id
     AND round      = first_round;

  matches_prev := first_round_matches;
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
          NULL,
          NULL,
          NULL
        )
        ON CONFLICT (tourney_id, id) DO NOTHING;
      END LOOP;
    END LOOP;
  END IF;

  EXECUTE 'DROP TABLE IF EXISTS pg_temp.tmp_first_round';

  CREATE TEMP TABLE tmp_first_round ON COMMIT DROP AS
  SELECT id, top_id, bot_id
  FROM public.draw_matches
  WHERE tourney_id = p_tourney_id
    AND round      = first_round;

  UPDATE public.draw_matches
  SET top_id    = NULL,
      bot_id    = NULL,
      winner_id = NULL
  WHERE tourney_id = p_tourney_id;

  UPDATE public.draw_matches dm
  SET top_id = tmp.top_id,
      bot_id = tmp.bot_id
  FROM tmp_first_round tmp
  WHERE dm.tourney_id = p_tourney_id
    AND dm.id        = tmp.id;

  SELECT LEFT(tourney_date::text, 4)::INT
  INTO match_year
  FROM estratego_v1.tournaments
  WHERE tourney_id = p_tourney_id
  LIMIT 1;

  FOR round_idx IN 1 .. array_length(rounds, 1) LOOP
    current_round := rounds[round_idx];
    next_round := CASE
      WHEN round_idx < array_length(rounds, 1)
        THEN rounds[round_idx + 1]
      ELSE NULL
    END;

    IF round_idx < first_round_idx THEN
      CONTINUE;
    END IF;

    -- Byes: solo un lado presente -> avanza directo, sin necesidad de probabilidad.
    UPDATE public.draw_matches
    SET winner_id = COALESCE(top_id, bot_id)
    WHERE tourney_id = p_tourney_id
      AND round      = current_round
      AND (top_id IS NULL) <> (bot_id IS NULL);

    -- Rellena la cache con las parejas de esta ronda que aun no conocemos
    -- (en una sola consulta, no una llamada por partido).
    INSERT INTO sim_prob_cache (player_lo, player_hi, prob_lo)
    SELECT pairs.lo, pairs.hi,
           (get_extended_prematch_summary(p_tourney_id, match_year, pairs.lo, pairs.hi) -> 'playerA' ->> 'win_probability')::FLOAT
    FROM (
      SELECT DISTINCT LEAST(top_id, bot_id) AS lo, GREATEST(top_id, bot_id) AS hi
      FROM public.draw_matches
      WHERE tourney_id = p_tourney_id
        AND round      = current_round
        AND top_id IS NOT NULL
        AND bot_id IS NOT NULL
    ) pairs
    WHERE NOT EXISTS (
      SELECT 1 FROM sim_prob_cache c
      WHERE c.player_lo = pairs.lo AND c.player_hi = pairs.hi
    )
    ON CONFLICT (player_lo, player_hi) DO NOTHING;

    -- Decide el ganador de todos los partidos de esta ronda en un solo UPDATE.
    UPDATE public.draw_matches dm
    SET winner_id = CASE
      WHEN random() < (CASE WHEN dm.top_id = c.player_lo THEN c.prob_lo ELSE 1 - c.prob_lo END)
        THEN dm.top_id
        ELSE dm.bot_id
      END
    FROM sim_prob_cache c
    WHERE dm.tourney_id = p_tourney_id
      AND dm.round      = current_round
      AND dm.top_id IS NOT NULL
      AND dm.bot_id IS NOT NULL
      AND c.player_lo = LEAST(dm.top_id, dm.bot_id)
      AND c.player_hi = GREATEST(dm.top_id, dm.bot_id);

    -- Propaga los ganadores de esta ronda a la siguiente en un solo UPDATE
    -- (agregando por partido destino para no chocar con dos filas origen
    -- actualizando la misma fila destino a la vez).
    IF next_round IS NOT NULL THEN
      WITH advances AS (
        SELECT
          next_round || '-' || ((split_part(id, '-', 2)::INT + 1) / 2) AS next_id,
          MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 1 THEN winner_id END) AS new_top,
          MAX(CASE WHEN split_part(id, '-', 2)::INT % 2 = 0 THEN winner_id END) AS new_bot
        FROM public.draw_matches
        WHERE tourney_id = p_tourney_id
          AND round      = current_round
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
  END LOOP;
END;
$function$;

DROP FUNCTION IF EXISTS public.simulate_next_round(text);

DROP FUNCTION IF EXISTS public.simulate_multiple_runs(text, integer, integer);
