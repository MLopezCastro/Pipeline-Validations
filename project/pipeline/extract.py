from pathlib import Path
import pandas as pd

def read_csv_from_bronze(root: Path, fname: str) -> pd.DataFrame:
    path = root / "data" / "bronze" / fname
    return pd.read_csv(path)

def write_csv_to_bronze(root: Path, df: pd.DataFrame, fname: str) -> None:
    path = root / "data" / "bronze"
    path.mkdir(parents=True, exist_ok=True)
    df.to_csv(path / fname, index=False)
