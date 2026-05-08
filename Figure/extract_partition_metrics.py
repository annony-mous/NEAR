from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook


DEFAULT_WORKBOOK = Path(__file__).resolve().parent / "Fig_all_new.xlsx"
DEFAULT_SHEET = "Sheet1"
DEFAULT_RANGE = "H31:M70"

# The numeric block in H31:M70 is stacked by shard configuration:
# 4 rows, then 5, 6, 7, 8, and 9 rows respectively.
GROUP_SIZES = [4, 5, 6, 7, 8, 9]


def gini(values: list[float]) -> float:
    clean_values = sorted(float(value) for value in values if value is not None)
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    weighted_sum = sum((index + 1) * value for index, value in enumerate(clean_values))
    count = len(clean_values)
    return (2 * weighted_sum) / (count * total) - (count + 1) / count


def top1_share(values: list[float]) -> float:
    clean_values = [float(value) for value in values if value is not None]
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    return max(clean_values) / total


def top2_share(values: list[float]) -> float:
    clean_values = sorted((float(value) for value in values if value is not None), reverse=True)
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    return sum(clean_values[:2]) / total


def coefficient_of_variation(values: list[float]) -> float:
    clean_values = [float(value) for value in values if value is not None]
    if not clean_values:
        return 0.0

    mean = sum(clean_values) / len(clean_values)
    if mean == 0:
        return 0.0

    variance = sum((value - mean) ** 2 for value in clean_values) / len(clean_values)
    return math.sqrt(variance) / mean


def normalized_entropy(values: list[float]) -> float:
    clean_values = [float(value) for value in values if value is not None and value >= 0]
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    shard_count = len(clean_values)
    if shard_count <= 1:
        return 1.0

    probabilities = [value / total for value in clean_values if value > 0]
    entropy = -sum(probability * math.log(probability) for probability in probabilities)
    return entropy / math.log(shard_count)


def effective_shard_count(values: list[float]) -> float:
    clean_values = [float(value) for value in values if value is not None and value >= 0]
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    return (total ** 2) / sum(value ** 2 for value in clean_values if value >= 0)


def hhi(values: list[float]) -> float:
    clean_values = [float(value) for value in values if value is not None and value >= 0]
    if not clean_values:
        return 0.0

    total = sum(clean_values)
    if total == 0:
        return 0.0

    return sum((value / total) ** 2 for value in clean_values)


def distribution_mismatch(left: list[float], right: list[float]) -> float:
    left_values = [float(value) for value in left if value is not None and value >= 0]
    right_values = [float(value) for value in right if value is not None and value >= 0]
    if not left_values or not right_values or len(left_values) != len(right_values):
        return 0.0

    left_total = sum(left_values)
    right_total = sum(right_values)
    if left_total == 0 or right_total == 0:
        return 0.0

    return sum(abs((left_value / left_total) - (right_value / right_total)) for left_value, right_value in zip(left_values, right_values))


def load_range_as_dataframe(workbook_path: Path, sheet_name: str, cell_range: str) -> pd.DataFrame:
    workbook = load_workbook(workbook_path, data_only=True)
    worksheet = workbook[sheet_name]
    rows = [[cell.value for cell in row] for row in worksheet[cell_range]]

    header = rows[0]
    data_rows = rows[1:]
    return pd.DataFrame(data_rows, columns=header)


def build_partition_table(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = ["blank_1", "blank_2", "total_users", "active_users", "contract_usage", "unique_contract_usage"]
    df = df[["total_users", "active_users", "contract_usage", "unique_contract_usage"]].copy()
    df = df.dropna(how="all").reset_index(drop=True)

    expected_rows = sum(GROUP_SIZES)
    if len(df) != expected_rows:
        raise ValueError(f"Expected {expected_rows} data rows in the range, found {len(df)}.")

    shard_config = []
    shard_id = []
    for shard_count in GROUP_SIZES:
        for shard_index in range(shard_count):
            shard_config.append(shard_count)
            shard_id.append(f"S{shard_index}")

    df.insert(0, "shard_id", shard_id)
    df.insert(0, "shard_config", shard_config)
    return df


def build_metric_summary(partition_df: pd.DataFrame) -> pd.DataFrame:
    summaries = []

    for shard_count, group in partition_df.groupby("shard_config", sort=True):
        total_users = group["total_users"].tolist()
        active_users = group["active_users"].tolist()
        contract_usage = group["contract_usage"].tolist()
        unique_contract_usage = group["unique_contract_usage"].tolist()

        total_users_sum = float(group["total_users"].sum())
        active_users_sum = float(group["active_users"].sum())
        contract_usage_sum = float(group["contract_usage"].sum())
        unique_contract_usage_sum = float(group["unique_contract_usage"].sum())

        active_ratio = active_users_sum / total_users_sum if total_users_sum else 0.0
        zero_active_user_shards = int((group["active_users"].fillna(0) == 0).sum())
        zero_total_user_shards = int((group["total_users"].fillna(0) == 0).sum())
        zero_contract_usage_shards = int((group["contract_usage"].fillna(0) == 0).sum())
        fully_idle_shards = int(
            ((group["active_users"].fillna(0) == 0) & (group["contract_usage"].fillna(0) == 0)).sum()
        )

        sorted_total_users = sorted((float(value) for value in total_users), reverse=True)
        sorted_contract_usage = sorted((float(value) for value in contract_usage), reverse=True)

        largest_shard_user_skew = sorted_total_users[0] / sorted_total_users[1] if len(sorted_total_users) > 1 and sorted_total_users[1] else float("inf")
        contract_hotspot_skew = (
            sorted_contract_usage[0] / sorted_contract_usage[1]
            if len(sorted_contract_usage) > 1 and sorted_contract_usage[1]
            else float("inf")
        )

        contract_per_active_user = contract_usage_sum / active_users_sum if active_users_sum else 0.0
        contract_per_total_user = contract_usage_sum / total_users_sum if total_users_sum else 0.0
        unique_contracts_per_active_user = unique_contract_usage_sum / active_users_sum if active_users_sum else 0.0
        contract_usage_per_unique_contract_user = (
            contract_usage_sum / unique_contract_usage_sum if unique_contract_usage_sum else 0.0
        )

        summaries.append(
            {
                "shard_config": shard_count,
                "gini_users": gini(total_users),
                "gini_active_users": gini(active_users),
                "gini_contracts": gini(contract_usage),
                "active_ratio": active_ratio,
                "largest_shard_user_share": top1_share(total_users),
                "largest_shard_contract_share": top1_share(contract_usage),
                "top2_user_share": top2_share(total_users),
                "top2_contract_share": top2_share(contract_usage),
                "zero_total_user_shards": zero_total_user_shards,
                "zero_active_user_shards": zero_active_user_shards,
                "zero_contract_usage_shards": zero_contract_usage_shards,
                "fully_idle_shards": fully_idle_shards,
                "largest_shard_user_skew": largest_shard_user_skew,
                "contract_hotspot_skew": contract_hotspot_skew,
                "cv_users": coefficient_of_variation(total_users),
                "cv_active_users": coefficient_of_variation(active_users),
                "cv_contracts": coefficient_of_variation(contract_usage),
                "entropy_users": normalized_entropy(total_users),
                "entropy_active_users": normalized_entropy(active_users),
                "entropy_contracts": normalized_entropy(contract_usage),
                "effective_user_shards": effective_shard_count(total_users),
                "effective_active_user_shards": effective_shard_count(active_users),
                "effective_contract_shards": effective_shard_count(contract_usage),
                "hhi_users": hhi(total_users),
                "hhi_contracts": hhi(contract_usage),
                "user_contract_mismatch": distribution_mismatch(total_users, contract_usage),
                "active_contract_mismatch": distribution_mismatch(active_users, contract_usage),
                "contract_per_total_user": contract_per_total_user,
                "contract_per_active_user": contract_per_active_user,
                "unique_contracts_per_active_user": unique_contracts_per_active_user,
                "contract_usage_per_unique_contract_user": contract_usage_per_unique_contract_user,
                "total_users_sum": int(total_users_sum),
                "active_users_sum": int(active_users_sum),
                "contract_usage_sum": int(contract_usage_sum),
                "unique_contract_usage_sum": int(unique_contract_usage_sum),
            }
        )

    metric_df = pd.DataFrame(summaries)
    ordered_columns = [
        "shard_config",
        "gini_users",
        "gini_contracts",
        "largest_shard_user_share",
        "largest_shard_contract_share",
        "effective_user_shards",
        "effective_contract_shards",
        "user_contract_mismatch",
        "active_contract_mismatch",
        # "gini_active_users",
        # "active_ratio",
        # "top2_user_share",
        # "top2_contract_share",
        # "largest_shard_user_skew",
        # "contract_hotspot_skew",
        # "cv_users",
        # "cv_active_users",
        # "cv_contracts",
        # "entropy_users",
        # "entropy_active_users",
        # "entropy_contracts",
        # "effective_active_user_shards",
        # "hhi_users",
        # "hhi_contracts",
        # "zero_total_user_shards",
        # "zero_active_user_shards",
        # "zero_contract_usage_shards",
        # "fully_idle_shards",
        # "contract_per_total_user",
        # "contract_per_active_user",
        # "unique_contracts_per_active_user",
        # "contract_usage_per_unique_contract_user",
        # "total_users_sum",
        # "active_users_sum",
        # "contract_usage_sum",
        # "unique_contract_usage_sum",
    ]
    return metric_df[ordered_columns]


def build_latex_table(metric_df: pd.DataFrame) -> str:
    lines = [
        r"\begin{table}[t]",
        r"\centering",
        r"\caption{Network-level imbalance and concentration metrics of user and contract distributions across shards for varying shard configurations. The mismatch columns compare total-user and active-user distributions against contract usage.}",
        r"\begin{tabular}{ccccccccc}",
        r"\hline",
        r" & \multicolumn{2}{c}{\textbf{Imbalance}}",
        r" & \multicolumn{2}{c}{\textbf{Top-Shard Share}}",
        r" & \multicolumn{2}{c}{\textbf{Effective Shards}}",
        r" & \multicolumn{2}{c}{\textbf{Mismatch vs Contract}} \\",
        r"\cline{2-9}",
        r"\textbf{Config}",
        r" & \textbf{User} & \textbf{Con.}",
        r" & \textbf{User} & \textbf{Con.}",
        r" & \textbf{User} & \textbf{Con.}",
        r" & \textbf{Total} & \textbf{Active} \\",
        r"\hline",
    ]

    for row in metric_df.itertuples(index=False):
        lines.append(
            f"{int(row.shard_config)} shards"
            f" & {row.gini_users:.3f} & {row.gini_contracts:.3f}"
            f" & {row.largest_shard_user_share:.3f} & {row.largest_shard_contract_share:.3f}"
            f" & {row.effective_user_shards:.3f} & {row.effective_contract_shards:.3f}"
            f" & {row.user_contract_mismatch:.3f} & {row.active_contract_mismatch:.3f} \\\\" 
        )

    lines.extend(
        [
            r"\hline",
            r"\end{tabular}",
            r"\label{tab:shard_imbalance_metrics}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract shard partition metrics from Sheet1!H31:M70 in Fig_all_new.xlsx."
    )
    parser.add_argument("--workbook", type=Path, default=DEFAULT_WORKBOOK)
    parser.add_argument("--sheet", default=DEFAULT_SHEET)
    parser.add_argument("--range", dest="cell_range", default=DEFAULT_RANGE)
    parser.add_argument(
        "--output-prefix",
        type=Path,
        help="Optional prefix for CSV outputs. Creates '<prefix>_partitions.csv' and '<prefix>_metrics.csv'.",
    )
    parser.add_argument(
        "--print-latex",
        action="store_true",
        help="Print a LaTeX table for the network-level metrics.",
    )
    parser.add_argument(
        "--save-tex",
        type=Path,
        help="Optional output path for the LaTeX table.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_df = load_range_as_dataframe(args.workbook, args.sheet, args.cell_range)
    partition_df = build_partition_table(raw_df)
    metric_df = build_metric_summary(partition_df)
    latex_table = build_latex_table(metric_df)

    print(metric_df.round(6).to_string(index=False))

    if args.print_latex:
        print("\nLaTeX table:")
        print(latex_table)

    if args.output_prefix:
        partition_path = args.output_prefix.with_name(f"{args.output_prefix.name}_partitions.csv")
        metric_path = args.output_prefix.with_name(f"{args.output_prefix.name}_metrics.csv")
        partition_df.to_csv(partition_path, index=False)
        metric_df.to_csv(metric_path, index=False)
        print(f"\nSaved partition values to: {partition_path}")
        print(f"Saved metric summary to: {metric_path}")

    if args.save_tex:
        args.save_tex.write_text(latex_table, encoding="utf-8")
        print(f"Saved LaTeX table to: {args.save_tex}")


if __name__ == "__main__":
    main()