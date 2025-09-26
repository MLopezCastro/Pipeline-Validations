# project/run_pipeline.py
from pathlib import Path
import pandas as pd

from pipeline.validations.runner import run_validations
from reporting import save_validation_report, save_json_log

# === Rutas robustas (independientes del cwd) ===
ROOT = Path(__file__).resolve().parents[1]   # carpeta raíz del repo
DATA = ROOT / "data"
LOGS = ROOT / "project" / "logs"
LOGS.mkdir(parents=True, exist_ok=True)

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
    df = pd.read_csv(csv_path)

    # 2) Ejecutar validaciones → lista de strings
    errors = run_validations(df)

    # 3) Guardar reportes 4.5 (CSV) y 4.6 (JSON)
    save_validation_report(errors, output_path=str(LOGS / "validation_report.csv"))
    save_json_log(errors,      output_path=str(LOGS / "validation_report.json"))

    # 4) Mensaje final
    print(f"OK 4.5/4.6 → reportes en:\n - {LOGS / 'validation_report.csv'}\n - {LOGS / 'validation_report.json'}\nFuente: {csv_path}")

if __name__ == "__main__":
    main()
