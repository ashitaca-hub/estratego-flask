# apps_script/prematch_bp.py
from __future__ import annotations
from flask import Blueprint, request, Response, render_template
import os, json, re
from main import get_matchup_payload  # si lo importas desde main


def _json_for_js(obj: dict) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    return s.replace("</", "<\\/")  # evita cerrar </script>

def _render_prematch_with_template(resp_dict: dict) -> str | None:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # raíz del repo
    tpl_path = os.environ.get(
        "PREMATCH_TEMPLATE",
        os.path.join(base_dir, "apps_script", "prematch_template.html"),
    )
    try:
        with open(tpl_path, "r", encoding="utf-8") as f:
            tpl = f.read()
    except Exception:
        return None

    js = f"const resp = {_json_for_js(resp_dict)};"
    if re.search(r"//\s*const\s+resp\s*=", tpl):
        # reemplaza bloque marcador en el template
        tpl = re.sub(r"//\s*const\s+resp\s*=\s*\{[\s\S]*?\};?", js, tpl, count=1)
    else:
        # si no hay marcador, lo inyecta
        if "</head>" in tpl:
            tpl = tpl.replace("</head>", f"<script>{js}</script>\n</head>", 1)
        elif "</body>" in tpl:
            tpl = tpl.replace("</body>", f"<script>{js}</script>\n</body>", 1)
        else:
            tpl = tpl + f"\n<script>{js}</script>\n"
    return tpl

def _fallback_html(resp_dict: dict) -> str:
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Prematch</title></head>
<body>
<pre id="json"></pre>
<script>
const resp = {_json_for_js(resp_dict)};
document.getElementById('json').textContent = JSON.stringify(resp, null, 2);
</script>
</body></html>"""

def make_prematch_bp(compute_fn):
    """
    compute_fn: función que recibe el payload (dict) y devuelve el MISMO dict
    que usas en /matchup (con prob_player, inputs, features, etc).
    """
prematch_bp = Blueprint(
    "prematch",
    __name__,
    template_folder="apps_script"  # 👈 importante: carpeta donde vive la plantilla
)

@prematch_bp.route("/matchup/prematch", methods=["POST"])
def matchup_prematch_html():
    try:
        data = request.get_json(force=True)
        resp_json = get_matchup_payload(data)

        # Ahora sí buscará en apps_script/prematch_template.html
        return render_template("prematch_template.html", json_data=resp_json)
    except Exception as e:
        logging.exception("Error en prematch")
        return jsonify({"error": "Internal server error"}), 500
