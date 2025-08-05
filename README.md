# estratego-flask
Repositorio para servidor puente en Render.com usando Flask

## Configuración de la API Key

Define la variable de entorno `SPORTRADAR_API_KEY` con tu clave de Sportradar antes de ejecutar la aplicación:

```bash
export SPORTRADAR_API_KEY="tu_api_key"
```

## Endpoint `/proximos_partidos`

Permite obtener los próximos partidos (aún no iniciados) de la temporada donde compite un jugador.

### Request

```
POST /proximos_partidos
{
  "jugador": "sr:competitor:123"
}
```

### Response

```
{
  "season_id": "sr:season:98765",
  "partidos": [
    {
      "start_time": "2024-05-01T10:00:00Z",
      "competitors": ["Jugador A", "Jugador B"],
      "round": "1st_round"
    }
  ]
}
```

## Endpoint `/proximos_partidos_por_torneo`

Permite obtener los próximos partidos de un torneo específico a partir de su nombre completo (incluyendo el año).

### Request

```
POST /proximos_partidos_por_torneo
{
  "torneo": "ATP Toronto, Canada Men Singles 2025"
}
```

### Response

```
{
  "season_id": "sr:season:98765",
"partidos": [
    {
      "start_time": "2025-08-01T10:00:00Z",
      "competitors": ["Jugador A", "Jugador B"],
      "round": "1st_round"
    }
]
}
```

## Evaluación de enfrentamientos

El endpoint raíz (`/`) permite evaluar un enfrentamiento entre dos jugadores. Acepta un campo opcional `superficie_objetivo` y responde con un resumen que incluye la pregunta:

* **¿Viene de cambio de superficie?** — se marca con `"✔"` cuando la superficie del último partido difiere de `superficie_objetivo` y con `"✘"` en caso contrario.

### Request

```
POST /
{
  "jugador": "sr:competitor:123",
  "rival": "sr:competitor:456",
  "superficie_objetivo": "hard"
}
```

### Response (fragmento)

```
{
  "cambio_superficie": "✔",
  ...
}
```
