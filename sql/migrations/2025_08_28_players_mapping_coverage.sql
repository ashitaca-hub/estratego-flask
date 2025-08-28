-- 2025_08_28_players_mapping_coverage.sql
-- Vistas para medir cobertura del mapeo SR y listar faltantes por prioridad.

CREATE OR REPLACE VIEW public.sr_mapping_coverage AS
WITH pop AS (
  SELECT
    COUNT(*) FILTER (WHERE l.ext_sportradar_id IS NOT NULL) AS mapped_players,
    COUNT(*) AS total_players
  FROM public.players_lookup l
),
pairs AS (
  SELECT
    LEAST(f.player_id, f.opponent_id)   AS p_low,
    GREATEST(f.player_id, f.opponent_id) AS p_high
  FROM public.fs_matches_long f
  WHERE f.match_date >= (current_date - interval '3 years')
  GROUP BY 1,2
),
pair_cov AS (
  SELECT
    COUNT(*) FILTER (
      WHERE l1.ext_sportradar_id IS NOT NULL
        AND l2.ext_sportradar_id IS NOT NULL
    ) AS mapped_pairs,
    COUNT(*) AS total_pairs
  FROM pairs u
  LEFT JOIN public.players_lookup l1 ON l1.player_id = u.p_high
  LEFT JOIN public.players_lookup l2 ON l2.player_id = u.p_low
)
SELECT
  p.mapped_players,
  p.total_players,
  ROUND(100.0 * p.mapped_players / NULLIF(p.total_players,0), 2) AS pct_players_mapped,
  c.mapped_pairs,
  c.total_pairs,
  ROUND(100.0 * c.mapped_pairs / NULLIF(c.total_pairs,0), 2)     AS pct_pairs_mapped_3y
FROM pop p CROSS JOIN pair_cov c;

-- Faltantes ordenados por “importancia” (frecuencia en fs_matches_long últimos 3 años)
CREATE OR REPLACE VIEW public.sr_missing_by_frequency AS
WITH freq AS (
  SELECT f.player_id, COUNT(*) AS n_matches
  FROM public.fs_matches_long f
  WHERE f.match_date >= (current_date - interval '3 years')
  GROUP BY f.player_id
),
miss AS (
  SELECT
    l.player_id,
    COALESCE(l.name, ('player_'||l.player_id)::text) AS name,
    f.n_matches
  FROM public.players_lookup l
  LEFT JOIN freq f ON f.player_id = l.player_id
  WHERE l.ext_sportradar_id IS NULL
)
SELECT *
FROM miss
ORDER BY COALESCE(n_matches,0) DESC, player_id;
