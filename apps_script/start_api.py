# apps_script/start_api.py
import os, sys, importlib, importlib.util, pathlib

def load_app(spec: str):
    """
    Admite:
      - 'main:app'  (módulo en sys.path)
      - 'path/to/file.py:app' (ruta a fichero)
    Devuelve el objeto WSGI 'app' (Flask) o ASGI si lo usas con Uvicorn.
    """
    modpart, _, attr = spec.partition(':')
    if not attr:
        attr = 'app'

    # Asegura que el root del repo está en sys.path
    ROOT = pathlib.Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    if modpart.endswith('.py') and os.path.exists(modpart):
        # Cargar desde ruta a fichero .py
        spec_file = importlib.util.spec_from_file_location("loaded_app", modpart)
        if not spec_file or not spec_file.loader:
            raise ImportError(f"No puedo cargar módulo desde {modpart}")
        mod = importlib.util.module_from_spec(spec_file)
        sys.modules["loaded_app"] = mod
        spec_file.loader.exec_module(mod)
    else:
        # Cargar como módulo importable (p.ej. 'main' o 'api.main')
        mod = importlib.import_module(modpart)

    if not hasattr(mod, attr):
        raise AttributeError(f"El módulo '{modpart}' no tiene el atributo '{attr}'")
    return getattr(mod, attr)

if __name__ == "__main__":
    # Por defecto buscamos 'main:app' en el root del repo.
    app_spec = os.environ.get("APP_MODULE", "main:app")
    app = load_app(app_spec)

    port = int(os.environ.get("PORT", "8080"))
    # Para Flask: app.run(...). Si fuera FastAPI/Starlette con Uvicorn,
    # arráncalo con uvicorn en el workflow.
    app.run(host="0.0.0.0", port=port, threaded=True)
