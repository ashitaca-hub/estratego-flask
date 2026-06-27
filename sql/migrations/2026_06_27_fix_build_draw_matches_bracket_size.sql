-- 2026_06_27_fix_build_draw_matches_bracket_size.sql
-- build_draw_matches usaba tournaments_info.draw_size (cuenta solo
-- jugadores reales, sin byes) como tamano del cuadro, en vez del numero
-- real de posiciones en draw_entries (que SI incluye los byes como filas
-- con player_id NULL / tag='BYE'). Para draws que no son potencia de 2
-- (ej. "Draw 28" = 32 posiciones con 4 byes), esto generaba solo la mitad
-- de los partidos de la ronda inicial y rondas posteriores con un numero
-- de partidos sin sentido (ver torneo 2026-8994, Mallorca).

CREATE OR REPLACE FUNCTION public.build_draw_matches(p_tournament_id text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  round_labels CONSTANT TEXT[] := ARRAY['R128','R64','R32','R16','QF','SF','F'];
  v_size       INT;
  v_round      TEXT;
  start_idx    INT;
  i            INT;
  top_pos      INT;
  bot_pos      INT;
  v_idx        INT;
  top_id       INT;
  bot_id       INT;
  matches_prev INT;
  r_idx        INT;
BEGIN
  -- Determine the real bracket size from draw_entries (incluye byes), no
  -- de tournaments_info.draw_size (que cuenta solo jugadores reales, sin
  -- byes, y por eso no coincide con el numero de posiciones del cuadro).
  SELECT COUNT(*)
    INTO v_size
    FROM public.draw_entries
   WHERE tourney_id = p_tournament_id;

  IF v_size IS NULL OR v_size = 0 THEN
    RAISE EXCEPTION 'no draw_entries found for tournament %', p_tournament_id;
  END IF;

  -- Drop existing matches for this tournament to ensure a clean rebuild.
  DELETE
    FROM public.draw_matches
   WHERE tourney_id = p_tournament_id;

  -- Pick the opening round label based on draw size (defaults to R16).
  IF v_size = 128 THEN
    v_round := 'R128';
  ELSIF v_size = 64 THEN
    v_round := 'R64';
  ELSIF v_size = 32 THEN
    v_round := 'R32';
  ELSIF v_size = 16 THEN
    v_round := 'R16';
  ELSIF v_size = 8 THEN
    v_round := 'QF';
  ELSIF v_size = 4 THEN
    v_round := 'SF';
  ELSIF v_size = 2 THEN
    v_round := 'F';
  ELSE
    v_round := 'R16';
  END IF;

  SELECT idx
    INTO start_idx
    FROM unnest(round_labels) WITH ORDINALITY AS t(lbl, idx)
    WHERE lbl = v_round
    LIMIT 1;

  v_idx := 1;
  i := 1;

  -- Generate pairings sequentially using the positions in draw_entries.
  WHILE i < v_size LOOP
    top_pos := i;
    bot_pos := i + 1;

    SELECT player_id
      INTO top_id
      FROM public.draw_entries
     WHERE tourney_id = p_tournament_id
       AND pos = top_pos;

    SELECT player_id
      INTO bot_id
      FROM public.draw_entries
     WHERE tourney_id = p_tournament_id
       AND pos = bot_pos;

    INSERT INTO public.draw_matches (id, tourney_id, round, top_id, bot_id)
    VALUES (
      CONCAT(v_round, '-', v_idx),
      p_tournament_id,
      v_round,
      top_id,
      bot_id
    );

    v_idx := v_idx + 1;
    i := i + 2;
  END LOOP;

  -- Pre-create empty matches for later rounds so simulations can advance.
  IF start_idx IS NOT NULL THEN
    matches_prev := (v_size + 1) / 2; -- round up number of matches in the opening round

    FOR r_idx IN start_idx + 1 .. array_length(round_labels, 1) LOOP
      matches_prev := (matches_prev + 1) / 2; -- round up half the previous round

      IF matches_prev <= 0 THEN
        EXIT;
      END IF;

      FOR i IN 1 .. matches_prev LOOP
        INSERT INTO public.draw_matches (id, tourney_id, round, top_id, bot_id, winner_id)
        VALUES (
          CONCAT(round_labels[r_idx], '-', i),
          p_tournament_id,
          round_labels[r_idx],
          NULL,
          NULL,
          NULL
        )
        ON CONFLICT (tourney_id, id) DO NOTHING;
      END LOOP;
    END LOOP;
  END IF;
END;
$function$;
