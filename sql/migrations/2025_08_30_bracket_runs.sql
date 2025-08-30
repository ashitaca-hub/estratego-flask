-- Tabla para persistir corridas de bracket
CREATE TABLE IF NOT EXISTS public.bracket_runs (
  id               bigserial PRIMARY KEY,
  created_at       timestamptz NOT NULL DEFAULT now(),
  tournament_name  text        NOT NULL,
  tournament_month int         NOT NULL,
  years_back       int         NOT NULL,
  mode             text        NOT NULL CHECK (mode IN ('deterministic','mc')),
  entrants         jsonb       NOT NULL,  -- lista original (name/id/seed opcional)
  result           jsonb       NOT NULL,  -- bracket completo (rondas, prob, winner)
  champion_id      int         NULL,
  champion_name    text        NULL,
  used_sr          boolean     NULL,      -- si hubo NOW/SR en la corrida
  api_version      text        NULL       -- (opcional) commit/tag del código
);

COMMENT ON TABLE public.bracket_runs IS 'Runs completos de simulaciones de bracket (auditoría y re-render).';

-- Índices de consulta más comunes
CREATE INDEX IF NOT EXISTS bracket_runs_created_desc_idx
  ON public.bracket_runs (created_at DESC);

CREATE INDEX IF NOT EXISTS bracket_runs_tourney_idx
  ON public.bracket_runs (tournament_name, tournament_month);

CREATE INDEX IF NOT EXISTS bracket_runs_result_gin
  ON public.bracket_runs USING GIN (result jsonb_path_ops);

-- Vista conveniente: últimos 50 runs
CREATE OR REPLACE VIEW public.bracket_runs_recent AS
SELECT id, created_at, tournament_name, tournament_month, years_back, mode,
       champion_id, champion_name, used_sr
FROM public.bracket_runs
ORDER BY created_at DESC
LIMIT 50;

GRANT SELECT ON public.bracket_runs, public.bracket_runs_recent TO anon, authenticated, service_role;
-- La inserción la hará el backend con la SERVICE ROLE (DATABASE_URL); no la exponemos al cliente.
