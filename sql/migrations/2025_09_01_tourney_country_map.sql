-- País por torneo (editable)
CREATE TABLE IF NOT EXISTS public.tourney_country_map(
  tourney_key   text PRIMARY KEY,   -- normalizado con public.norm_tourney(name)
  country_code  text NOT NULL       -- ISO-3 preferido (USA, ESP, FRA, ...), también vale ISO-2 si eres consistente
);

GRANT SELECT ON public.tourney_country_map TO anon, authenticated, service_role;

-- Semillas mínimas
INSERT INTO public.tourney_country_map (tourney_key, country_code) VALUES
  ('cincinnati', 'USA'),
  ('us open', 'USA'),
  ('paris masters', 'FRA'),
  ('indian wells', 'USA')
ON CONFLICT (tourney_key) DO NOTHING;
