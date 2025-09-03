# apps_script/prematch_bp.py
from __future__ import annotations

import json
from flask import Blueprint, request, render_template

# Usamos la plantilla que vive en este mismo directorio (apps_script/)
bp = Blueprint("prematch", __name__, template_folder=".")

@bp.route("/prematch", methods=["POST"])
def prematch():
    """
    Renderiza el Prematch HTML moderno.
    - Calcula el payload con la misma lógica de /matchup
    - Enriquece con extras (rank, ytd, etc.)
    - Inyecta el JSON en la plantilla como window.resp
    """
    payload = request.get_json(force=True, silent=True) or {}

    # Import tardío para evitar import circular con main.py
    from main import _compute_matchup_payload, enrich_resp_with_extras

    out = _compute_matchup_payload(payload)
    out = enrich_resp_with_extras(out)

    # Se inyectará como window.resp en la plantilla
    resp_json = json.dumps(out, ensure_ascii=False)

    return render_template("prematch_template.html", json_data=resp_json)

# Alias por compatibilidad si en main.py importas 'prematch_bp'
prematch_bp = bp
