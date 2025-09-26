# project/plot_errors_by_day.py
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "project" / "logs"
csv_path = LOGS / "validation_report.csv"

# 1) Simular corridas con fechas distintas (opcional; comenta si ya tenés historial)
#    Duplicamos el archivo cambiando timestamp para 3 días atrás
df = pd.read_csv(csv_path)
if not df.empty:
    df2 = df.copy(); df2["timestamp"] = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    df3 = df.copy(); df3["timestamp"] = pd.Timestamp.now().normalize() - pd.Timedelta(days=2)
    hist = pd.concat([df, df2, df3], ignore_index=True)
else:
    hist = df

# 2) Agrupar por día y contar errores
hist["day"] = pd.to_datetime(hist["timestamp"]).dt.date
counts = hist.groupby("day")["error"].count().reset_index(name="errors")

# 3) Graficar
plt.figure()
plt.plot(counts["day"], counts["errors"], marker="o")
plt.title("Errores de validación por día")
plt.xlabel("Día")
plt.ylabel("Cantidad de errores")
plt.xticks(rotation=45)
plt.tight_layout()
out = LOGS / "errors_by_day.png"
plt.savefig(out)
print(f"Gráfico guardado en: {out}")
