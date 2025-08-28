-- Índices clave para /matchup y fs_*_asof
CREATE INDEX IF NOT EXISTS fs_matches_long_pid_date_idx
  ON public.fs_matches_long (player_id, match_date);

CREATE INDEX IF NOT EXISTS fs_matches_long_date_idx
  ON public.fs_matches_long (match_date);

CREATE INDEX IF NOT EXISTS fs_matches_long_tourney_idx
  ON public.fs_matches_long (lower(tournament_name));

CREATE INDEX IF NOT EXISTS court_speed_keyed_key_idx
  ON public.court_speed_rankig_norm_compat_keyed (tourney_key);
