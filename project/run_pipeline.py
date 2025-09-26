# project/run_pipeline.py
from pathlib import Path
import pandas as pd

from pipeline.validations.runner import run_validations
from reporting import save_validation_report

# === Rutas robustas (independientes del cwd) ===
ROOT = Path(__file__).resolve().parents[1]       # carpeta raíz del repo
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
    raise FileNotFoundError(f"No encontré el CSV. Probé:\n- " + "\n- ".join(map(str, CANDIDATES)))

def main():
    df = pd.read_csv(csv_path)
    errors = run_validations(df)
    save_validation_report(errors, output_path=str(LOGS / "validation_report.csv"))
    print(f"OK 4.5 → reporte en {LOGS / 'validation_report.csv'}\nFuente: {csv_path}")

if __name__ == "__main__":
    main()
