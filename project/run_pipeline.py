# project/run_pipeline.py
from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

from pipeline.validations.runner import run_validations
from reporting import save_validation_report, save_json_log

# === Rutas robustas (independientes del cwd) ===
ROOT = Path(__file__).resolve().parents[1]   # carpeta raíz del repo
DATA = ROOT / "data"
LOGS = ROOT / "project" / "logs"
LOGS.mkdir(parents=True, exist_ok=True)

# === Logging (consola + archivo) ===
LOG_FILE = LOGS / "pipeline.log"  # si querés .txt, cambiá la extensión
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)
if not logger.handlers:  # evitar handlers duplicados en ejecuciones repetidas
    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    ch = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    fh.setFormatter(fmt); ch.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(ch)

# Probar varias ubicaciones del CSV
CANDIDATES = [
    DATA / "ventas.csv",
    DATA / "bronze" / "ventas.csv",
]
csv_path = next((p for p in CANDIDATES if p.exists()), None)
if not csv_path:
    raise FileNotFoundError(
        "No encontré el CSV. Probé:\n- " + "\n- ".join(map(str, CANDIDATES))
    )

def main():
    # 1) Cargar datos
    logger.info(f"Leyendo CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)

    # 2) Ejecutar validaciones → lista de strings
    errors = run_validations(df)
    logger.info(f"Validaciones ejecutadas. Errores detectados: {len(errors)}")

    # Logueo cada error como WARNING (puede cambiarse a ERROR según severidad)
    for e in errors:
        logger.warning(e)

    # 3) Guardar reportes 4.5 (CSV) y 4.6 (JSON)
    save_validation_report(errors, output_path=str(LOGS / "validation_report.csv"))
    save_json_log(errors,      output_path=str(LOGS / "validation_report.json"))
    logger.info(f"Reportes escritos en: {LOGS}")

    # 4) Fin
    logger.info("Ejecución finalizada.")

if __name__ == "__main__":
    main()
