from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_WORKBOOK = Path("dataset/Figure/Fig_all_new.xlsx")
DEFAULT_SHEET = "Q_TPSxGasx(1+CTx)"
CORE_NUMERIC_COLUMNS = ["TPS", "Average gas (TGas)", "Demand", "Latency"]


def load_queue_sheet(workbook: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(workbook, sheet_name=sheet_name)
    first_column_name = df.columns[0]
    if not str(first_column_name).strip():
        df = df.rename(columns={first_column_name: "Scenario"})
    else:
        df = df.rename(columns={first_column_name: "Scenario"})
    df["Scenario"] = df["Scenario"].ffill()
    return df


def build_core_dataset(df: pd.DataFrame) -> pd.DataFrame:
    core = df[["Scenario", "Shard ID", *CORE_NUMERIC_COLUMNS]].copy()
    for column in CORE_NUMERIC_COLUMNS:
        core[column] = pd.to_numeric(core[column], errors="coerce")
    core = core.dropna(subset=["Shard ID"])
    core = core.dropna(subset=CORE_NUMERIC_COLUMNS, how="all")
    return core


def find_queue_pairs(df: pd.DataFrame) -> list[tuple[str, str, str]]:
    pairs: list[tuple[str, str, str]] = []
    columns = list(df.columns)

    for index, column in enumerate(columns[:-1]):
        if not str(column).startswith("Shard Load"):
            continue

        next_column = columns[index + 1]
        if not str(next_column).startswith("Avg Lat"):
            continue

        scenario_label = str(df.iloc[0, index + 1]).strip()
        pairs.append((scenario_label, column, next_column))

    return pairs


def compute_queue_correlations(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    for scenario_label, load_column, latency_column in find_queue_pairs(df):
        pair_df = df[[load_column, latency_column]].copy()
        pair_df[load_column] = pd.to_numeric(pair_df[load_column], errors="coerce")
        pair_df[latency_column] = pd.to_numeric(pair_df[latency_column], errors="coerce")
        pair_df = pair_df.dropna()

        correlation = pair_df[load_column].corr(pair_df[latency_column])
        rows.append(
            {
                "scenario": scenario_label,
                "rows_used": len(pair_df),
                "pearson_correlation": correlation,
            }
        )

    return pd.DataFrame(rows)


def print_report(df: pd.DataFrame) -> None:
    core = build_core_dataset(df)

    print("Overall correlations for Queue sheet core metrics")
    print(core[CORE_NUMERIC_COLUMNS].corr().round(4).to_string())
    print()

    print("Per-scenario correlations for Queue sheet core metrics")
    for scenario, group in core.groupby("Scenario", sort=False):
        print(f"\n[{scenario}]")
        print(group[CORE_NUMERIC_COLUMNS].corr().round(4).to_string())

    print()
    print("Queue load vs latency Pearson correlations")
    print(compute_queue_correlations(df).round(4).to_string(index=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute correlation values from the Queue sheet in Fig_all_new.xlsx."
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=DEFAULT_WORKBOOK,
        help="Path to the Excel workbook. Default: dataset/Figure/Fig_all_new.xlsx",
    )
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help="Excel sheet name. Default: Queue",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_queue_sheet(args.workbook, args.sheet)
    print_report(df)


if __name__ == "__main__":
    main()