# 📖 Flujo de carga de cuadros ATP en Estratego

## 1. Objetivo
Automatizar el proceso de:
- Descargar cuadros de torneos ATP desde PDFs oficiales (`protennislive.com`).
- Parsearlos a CSV limpio.
- Cargar en **staging** (`stg_draw_entries_by_name`).
- Resolver jugadores contra `players_dim`.
- Poblar tablas finales (`draw_entries` y `draw_matches`).

De esta manera, desde un PDF en crudo pasamos a tener un **cuadro estructurado y simulable en la app**.

---

## 2. Scripts desarrollados

### 🔹 `get_atp_draws.py`
- Descarga y lee PDFs (`mds.pdf`).
- Extrae líneas con `pdfplumber`.
- Genera un **CSV limpio** con columnas:
  - `pos` (posición en el cuadro)
  - `player_name`
  - `seed`
  - `tag` (`Qualifier`, `WC`, `BYE`, `PR`, `LL`)
  - `country`

👉 Ejemplo de salida:
```csv
1,"ALCARAZ, Carlos",1,,ESP
2,"BAEZ, Sebastian",,,ARG
7,,,Qualifier,
10,"MOCHIZUKI, Shintaro",,WC,JPN
```

---

### 🔹 `load_draw_to_staging.py`
- Lee el CSV generado.
- Añade automáticamente `tourney_id` (ej: `2025-329`).
- Inserta filas en `stg_draw_entries_by_name` mediante **Supabase REST API**.
- Se corrigieron problemas de:
  - `NaN` → `None` para JSON válido.
  - `tourney_id` ausente.
  - `Qualifier` sin `player_name`.

👉 Resultado: staging refleja fielmente lo que trae el PDF.

---

### 🔹 `load_from_staging.py`
- Lee filas **no procesadas** de staging (`processed_at IS NULL`).
- Resuelve `player_name` → `player_id` con búsqueda `ILIKE` en `players_dim`.
  - Intenta `"Apellido, Nombre"`.
  - Intenta `"Nombre Apellido"`.
  - Case-insensitive.
- Inserta en `draw_entries` con:
  - `pos`
  - `player_id` (o NULL si no resuelto)
  - `seed`
  - `tag` (`Qualifier`, `WC`, `UNRESOLVED`)
- Marca staging como procesado (`processed_at`).

---

### 🔹 `build_draw_matches` (función SQL)
- Genera los partidos iniciales (`R32`, `R64`, etc.) en `draw_matches`.
- Se simplificó para usar **`draw_size` desde tournaments** en lugar de tablas duplicadas.
- Inserta enfrentamientos según posiciones (`1 vs 32`, `2 vs 31`, etc.).

---

## 3. Flujo de trabajo

1. **Upsert Tournament**  
   → `upsert_tournament.py` inserta/actualiza torneo en `tournaments`.

2. **Get Draw (PDF → CSV)**  
   → `get_atp_draws.py data/mds_2025_329.pdf data/draw_329.csv`.

3. **Load CSV to Staging**  
   → `load_draw_to_staging.py data/draw_329.csv`.

4. **Migrate Staging → Draw Entries**  
   → `load_from_staging.py`.

5. **Build Matches**  
   → RPC a `build_draw_matches(p_tourney_id := '2025-329')`.

---

## 4. Estado actual
✅ PDF → CSV correcto.  
✅ CSV → staging sin errores.  
✅ Staging → draw_entries con lógica de resolución de jugadores.  
✅ Falta validar resolución de algunos nombres (`players_dim` vs PDF).  
✅ Matches se pueden generar desde la función SQL.  

---

## 5. Próximos pasos
- Afinar normalización de nombres para resolver más jugadores.
- Montar **workflow único de GitHub Actions** que ejecute todo en orden automático (1→5).
- Opcional: integrar validaciones (ej: número esperado de seeds, BYEs, etc.).