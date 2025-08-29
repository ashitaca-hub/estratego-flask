-- Requiere pgcrypto para gen_random_uuid (usado en B)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- A.1) Tabla de caché de /matchup
CREATE TABLE IF NOT EXISTS public.matchup_cache (
  player_id     integer     NOT NULL,
  opponent_id   integer     NOT NULL,
  tourney_key   text        NOT NULL,   -- normalizado con public.norm_tourney()
  mon           integer     NOT NULL,   -- mes del torneo
  surface       text,                   -- surface detectada
  speed_bucket  text,                   -- Slow/Medium/Fast (si aplica)
  speed_key     text        GENERATED ALWAYS AS (lower(COALESCE(speed_bucket,''))) STORED,
  years_back    integer     NOT NULL,
  using_sr      boolean     NOT NULL DEFAULT false, -- se usó Sportradar (ytd/last10/ranking...)?
  prob_player   double precision NOT NULL,
  features      jsonb       NOT NULL,
  flags         jsonb       NOT NULL,
  weights_hist  jsonb,
  sources       jsonb,                  -- opcional (qué endpoints SR se llamaron, etc.)
  created_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz,
  PRIMARY KEY (player_id, opponent_id, tourney_key, mon, years_back, using_sr, speed_key)
);

-- Índices auxiliares
CREATE INDEX IF NOT EXISTS matchup_cache_created_idx ON public.matchup_cache (created_at DESC);
CREATE INDEX IF NOT EXISTS matchup_cache_expires_idx ON public.matchup_cache (expires_at);

-- A.2) Lectura desde caché (retorna JSON o NULL si no hay / expirado)
CREATE OR REPLACE FUNCTION public.get_matchup_cache_json(
  p_player_id   integer,
  p_opponent_id integer,
  p_tourney_key text,
  p_mon         integer,
  p_speed_bucket text,
  p_years_back  integer,
  p_using_sr    boolean
) RETURNS jsonb
LANGUAGE sql
AS $$
  SELECT to_jsonb(t) FROM (
    SELECT prob_player, features, flags, weights_hist, sources
    FROM public.matchup_cache
    WHERE player_id = p_player_id
      AND opponent_id = p_opponent_id
      AND tourney_key = p_tourney_key
      AND mon = p_mon
      AND speed_key = lower(COALESCE(p_speed_bucket,''))
      AND years_back = p_years_back
      AND using_sr = p_using_sr
      AND (expires_at IS NULL OR expires_at > now())
    LIMIT 1
  ) t;
$$;

-- A.3) Upsert en caché con TTL (segundos)
CREATE OR REPLACE FUNCTION public.put_matchup_cache_json(
  p_player_id    integer,
  p_opponent_id  integer,
  p_tourney_key  text,
  p_mon          integer,
  p_surface      text,
  p_speed_bucket text,
  p_years_back   integer,
  p_using_sr     boolean,
  p_prob_player  double precision,
  p_features     jsonb,
  p_flags        jsonb,
  p_weights_hist jsonb DEFAULT NULL,
  p_sources      jsonb DEFAULT NULL,
  p_ttl_seconds  integer DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_expires timestamptz;
BEGIN
  v_expires := CASE WHEN p_ttl_seconds IS NULL THEN NULL
                    ELSE now() + make_interval(secs => p_ttl_seconds)
               END;

  INSERT INTO public.matchup_cache
    (player_id, opponent_id, tourney_key, mon, surface, speed_bucket, years_back, using_sr,
     prob_player, features, flags, weights_hist, sources, expires_at)
  VALUES
    (p_player_id, p_opponent_id, p_tourney_key, p_mon, p_surface, p_speed_bucket, p_years_back, p_using_sr,
     p_prob_player, p_features, p_flags, p_weights_hist, p_sources, v_expires)
  ON CONFLICT (player_id, opponent_id, tourney_key, mon, years_back, using_sr, speed_key)
  DO UPDATE SET
     prob_player = EXCLUDED.prob_player,
     features    = EXCLUDED.features,
     flags       = EXCLUDED.flags,
     weights_hist= EXCLUDED.weights_hist,
     sources     = EXCLUDED.sources,
     surface     = EXCLUDED.surface,
     speed_bucket= EXCLUDED.speed_bucket,
     expires_at  = COALESCE(EXCLUDED.expires_at, public.matchup_cache.expires_at),
     created_at  = now();
END;
$$;
