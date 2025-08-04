# estratego-flask
Repositorio para servidor puente en Render.com usando Flask

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
