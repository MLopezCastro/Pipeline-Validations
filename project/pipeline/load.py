from pathlib import Path
import pandas as pd

def write_silver(root: Path, df: pd.DataFrame, fname: str) -> None:
    path = root / "data" / "silver"
    path.mkdir(parents=True, exist_ok=True)
    df.to_csv(path / fname, index=False)

def write_gold(root: Path, df: pd.DataFrame, fname: str) -> None:
    path = root / "data" / "gold"
    path.mkdir(parents=True, exist_ok=True)
    df.to_csv(path / fname, index=False)

def write_quarantine(root: Path, df: pd.DataFrame, fname: str) -> None:
    path = root / "data" / "quarantine"
    path.mkdir(parents=True, exist_ok=True)
    df.to_csv(path / fname, index=False)
