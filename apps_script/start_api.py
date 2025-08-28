# scripts/start_api.py
import os
from main import app

if __name__ == "__main__":
    # En CI usamos 8080; respeta PORT si está definido
    port = int(os.environ.get("PORT", "8080"))
    # threaded=True evita bloqueos si el handler hace llamadas internas
    app.run(host="0.0.0.0", port=port, threaded=True)
