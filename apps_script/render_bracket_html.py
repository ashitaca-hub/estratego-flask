# apps_script/render_bracket_html.py
import json, os, html, pathlib, csv

BRACKET_JSON = os.environ.get("BRACKET_JSON", "/tmp/bracket.json")
OUT_DIR = os.environ.get("OUT_DIR", "artifacts")
OUT_HTML = os.path.join(OUT_DIR, "bracket.html")
OUT_CSV  = os.path.join(OUT_DIR, "bracket_matches.csv")  # copia si existe

pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

def load_bracket(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def pct(x): 
    try:
        return f"{float(x)*100:.1f}%"
    except Exception:
        return "–"

def color_for_prob(p):
    """De 0..1 a color: rojo (0) -> ámbar (0.5) -> verde (1)."""
    p = max(0.0, min(1.0, float(p)))
    # Interpolación simple: rojo->ámbar (0..0.5), ámbar->verde (0.5..1)
    if p < 0.5:
        t = p / 0.5
        r,g,b = 255, int(165*t + 0*(1-t)), 0
    else:
        t = (p - 0.5) / 0.5
        r,g,b = int(255*(1-t) + 255*t*0.1), int(165*(1-t) + 200*t), int(0*(1-t) + 80*t)
    return f"rgb({r},{g},{b})"

def prob_bar_html(prob_a):
    """Dos barras (A y B) con ancho relativo."""
    try:
        p = float(prob_a)
    except Exception:
        p = 0.5
    pa = max(0.0, min(1.0, p))
    pb = 1.0 - pa
    ca = color_for_prob(pa)
    cb = color_for_prob(pb)
    return f"""
      <div class="bars">
        <div class="bar a" style="width:{pa*100:.1f}%; background:{ca}"></div>
        <div class="bar b" style="width:{pb*100:.1f}%; background:{cb}"></div>
      </div>
    """

def render_round(round_obj):
    cards = []
    for m in round_obj.get("matches", []):
        a = html.escape(str(m.get("a","")))
        b = html.escape(str(m.get("b","")))
        pa = m.get("prob_a", 0.5)
        w  = html.escape(str(m.get("winner","")))
        cards.append(f"""
          <div class="match">
            <div class="row">
              <span class="name">{a}</span>
              <span class="prob">{pct(pa)}</span>
            </div>
            {prob_bar_html(pa)}
            <div class="row second">
              <span class="name">{b}</span>
              <span class="prob">{pct(1-float(pa))}</span>
            </div>
            <div class="winner">Winner: <strong>{w}</strong></div>
          </div>
        """)
    return "\n".join(cards)

def render_html(data):
    bracket = data.get("bracket") or data.get("example_bracket") or []
    mode    = data.get("mode", "deterministic")
    tname   = data.get("tournament", {}).get("name", "Tournament")
    mon     = data.get("tournament", {}).get("month", "?")
    yb      = data.get("years_back", "?")
    champ   = data.get("champion") or data.get("example_champion")
    champ_name = champ.get("name") if isinstance(champ, dict) else str(champ)

    rounds = []
    for r in bracket:
        rounds.append(f"""
          <div class="column">
            <div class="round-title">Round {int(r.get('round',0))}</div>
            {render_round(r)}
          </div>
        """)

    legend = """
      <div class="legend">
        <div><span class="swatch" style="background:#ff4d4d"></span> Underdog</div>
        <div><span class="swatch" style="background:#ffae00"></span> 50/50</div>
        <div><span class="swatch" style="background:#3cb371"></span> Favorito</div>
      </div>
    """

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Bracket — {html.escape(tname)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  :root {{
    --card-bg: #0f172a;      /* slate-900 */
    --card-fg: #e2e8f0;      /* slate-200 */
    --muted:   #94a3b8;      /* slate-400 */
    --border:  #1f2937;      /* slate-800 */
  }}
  body {{
    margin: 0; padding: 24px;
    font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, "Helvetica Neue", Arial;
    background: #0b1022; color: var(--card-fg);
  }}
  h1 {{
    margin: 0 0 8px 0; font-size: 22px;
  }}
  .meta {{
    color: var(--muted); margin-bottom: 16px; font-size: 14px;
  }}
  .legend {{
    display:flex; gap:16px; align-items:center; margin: 12px 0 24px 0; color: var(--muted);
  }}
  .legend .swatch {{ display:inline-block; width:16px; height:10px; border-radius:3px; margin-right:6px; }}
  .grid {{
    display: grid;
    grid-template-columns: repeat({len(bracket)}, minmax(260px, 1fr));
    gap: 16px;
    align-items: start;
  }}
  .column {{ display: flex; flex-direction: column; gap: 12px; }}
  .round-title {{ color: var(--muted); font-size: 13px; margin-bottom: 4px; }}
  .match {{
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 10px 12px;
    box-shadow: 0 1px 0 rgba(255,255,255,0.04) inset, 0 8px 30px rgba(0,0,0,0.35);
  }}
  .row {{ display:flex; justify-content:space-between; align-items:center; gap:8px; }}
  .row.second {{ margin-top: 8px; }}
  .name {{ font-weight: 600; font-size: 14px; }}
  .prob {{ color: var(--muted); font-variant-numeric: tabular-nums; }}
  .bars {{
    display:flex; height: 8px; margin: 8px 0 0 0; border-radius: 6px; overflow: hidden; border: 1px solid var(--border);
  }}
  .bar {{ height: 100%; }}
  .bar.a {{ border-right: 1px solid var(--border); }}
  .winner {{ margin-top: 10px; color: var(--muted); font-size: 12px; }}
  .foot {{ margin-top: 24px; color: var(--muted); font-size: 12px; }}
</style>
</head>
<body>
  <h1>{html.escape(tname)} — Bracket</h1>
  <div class="meta">mode=<b>{html.escape(mode)}</b> · month={html.escape(str(mon))} · years_back={html.escape(str(yb))} · champion=<b>{html.escape(str(champ_name))}</b></div>
  {legend}
  <div class="grid">
    {''.join(rounds)}
  </div>
  <div class="foot">Generado automáticamente desde bracket.json</div>
</body>
</html>"""

def maybe_copy_csv():
    src = "/tmp/bracket_matches.csv"
    if os.path.exists(src):
        dst = OUT_CSV
        with open(src, "r", encoding="utf-8") as i, open(dst, "w", encoding="utf-8") as o:
            o.write(i.read())

def main():
    data = load_bracket(BRACKET_JSON)
    html_str = render_html(data)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html_str)
    maybe_copy_csv()
    print(f"Saved: {OUT_HTML}")
    if os.path.exists(OUT_CSV):
        print(f"Saved: {OUT_CSV}")

if __name__ == "__main__":
    main()
