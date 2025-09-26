import os
import pandas as pd
from datetime import datetime

def save_validation_report(errors: list, output_path="logs/validation_report.csv"):
    """
    Guarda un CSV con 2 columnas: timestamp y error.
    - Crea la carpeta logs/ si no existe.
    - Agrega filas (no sobrescribe el archivo).
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = [{"timestamp": now, "error": str(e)} for e in errors]
    df = pd.DataFrame(rows)
    header = not os.path.exists(output_path)
    df.to_csv(output_path, index=False, mode="a", header=header)
