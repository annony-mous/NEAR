from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_WORKBOOK = Path(__file__).resolve().parent / "Fig_all_new.xlsx"
DEFAULT_SHEETS = ("tps", "latency")


def build_section_summary(workbook_path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(workbook_path, sheet_name=sheet_name)

    shard_columns = [column for column in df.columns if str(column).startswith("S")]
    if not shard_columns:
        raise ValueError(f"No shard columns found in sheet '{sheet_name}'.")

    data = df.copy()
    data["active_shard_count"] = data[shard_columns].notna().sum(axis=1)
    data = data[data["active_shard_count"] > 0].copy()

    if data.empty:
        raise ValueError(f"Sheet '{sheet_name}' does not contain any populated shard rows.")

    data["section_id"] = (data["active_shard_count"] != data["active_shard_count"].shift()).cumsum()
    data["shard_sum"] = data[shard_columns].sum(axis=1, skipna=True)

    summary = (
        data.groupby("section_id", as_index=False)
        .agg(
            active_shards=("active_shard_count", "first"),
            rows=("section_id", "size"),
            epoch_start=("epoch_index", "min"),
            epoch_end=("epoch_index", "max"),
            mean_net=("Net", "mean"),
            median_net=("Net", "median"),
            min_net=("Net", "min"),
            max_net=("Net", "max"),
            mean_shard_sum=("shard_sum", "mean"),
        )
        .sort_values("section_id")
    )
    summary.insert(0, "sheet", sheet_name)

    return summary


def build_multi_sheet_summary(workbook_path: Path, sheet_names: list[str]) -> pd.DataFrame:
    summaries = [build_section_summary(workbook_path, sheet_name) for sheet_name in sheet_names]
    return pd.concat(summaries, ignore_index=True)


def format_summary(summary: pd.DataFrame) -> str:
    formatted = summary.copy()
    float_columns = [
        "epoch_start",
        "epoch_end",
        "mean_net",
        "median_net",
        "min_net",
        "max_net",
        "mean_shard_sum",
    ]
    formatted[float_columns] = formatted[float_columns].round(6)
    return formatted.to_string(index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize sheet sections based on populated shard columns."
    )
    parser.add_argument(
        "--workbook",
        type=Path,
        default=DEFAULT_WORKBOOK,
        help=f"Path to the Excel workbook. Defaults to {DEFAULT_WORKBOOK.name} in this folder.",
    )
    parser.add_argument(
        "--sheet",
        nargs="+",
        default=list(DEFAULT_SHEETS),
        help="One or more sheet names to read. Defaults to: tps latency.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to save the combined section summary as CSV.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = build_multi_sheet_summary(args.workbook, args.sheet)
    print(format_summary(summary))

    if args.output:
        summary.to_csv(args.output, index=False)
        print(f"\nSaved CSV summary to: {args.output}")


if __name__ == "__main__":
    main()