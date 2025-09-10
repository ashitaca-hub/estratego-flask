# Estratego — Prematch: Plan, Datos y Estado

## 1) Propósito
Estrategia para ganar a largo plazo en apuestas de tenis. El **Prematch** muestra matchup con señales históricas + actuales para estimar probabilidad y detectar valor.

## 2) Fuentes de datos
- **SportRadar (2024–2025)** → `public.fs_matches_long` (player_id, winner_id, match_date, tournament_name, ext_event_id, [surface?], [speed_bucket?]).
- **Histórico Estratego** → `estratego_v1.matches`, `estratego_v1.tournaments (tourney_id, name, level)`.
- **Ranking actual** → `public.rankings_snapshot_int` (snapshot_date, player_id, rank, points).
- **YTD** → `public.v_player_ytd_now_int` (player_id, wr).
- **Mapa país torneo** → `public.tourney_country_map(tourney_key, country_code)`.
- **Defensa puntos/título** → `public.player_defense_prev_year(tourney_key, player_id, points, title_code)`.

## 3) Claves y mapeos
- `tourney_key` := `norm_tourney(tournament_name)` en fs + `norm_tourney(name)` en histórico.
- **Sugerencia**: crear `tourney_key_map(sr_event_id, tourney_key, tourney_id, manual_override)`.

## 4) Dependencias Prematch (DoD)
- **Extras**: nombre, bandera (ISO2), ranking (rank), YTD (wr), local (country_player == country_tourney).
- **Señales HIST**: Δmes, Δsuperficie, Δvelocidad → pesos [0.5, 2.0, 2.0].
- **Señales NOW**: rank_norm, ytd, last10, inactividad, h2h.
- **Probabilidad**: combinación HIST+NOW → `sigmoid(score_total)`. Fallback si HIST=0: rank+YTD.
- **Badges**: `def_points`, `def_title` (champ/runner).

## 5) Estado (hoy)
- Nombres ✅ | Ranking ✅ | YTD ✅ | Banderas ✅ | Local ✅  
- Señales HIST (Δmes/Δsurf/Δspeed) ⚠️ (necesitan fuente surface/velocidad o mapping)  
- Probabilidad ≈ 50% ⚠️ (activar fórmula HIST+NOW; fallback ya añadido)  
- Defensa puntos/título ⚠️ (pendiente backfill estable basado en esquema real)

## 6) Workflows
- `db_inventory.yml` → CSVs con esquema, conteos, join fs↔histórico, búsqueda.
- `db_validate_prematch.yml` → CSV PASS/FAIL dependencias.
- (próximo) `db_defense_points.yml` → crear vista/tabla con campeón/runner y puntos.

## 7) Próximos pasos
1. Ejecutar `db_inventory.yml` y revisar `join_fs_et_by_key.csv`.
2. Decidir `tourney_key` y crear `tourney_key_map` (si hay colisiones).
3. Armar `defense_points` con columnas reales (evitar supuestos).
4. Integrar **HIST** en `/matchup` y activar la **probabilidad** con pesos.
