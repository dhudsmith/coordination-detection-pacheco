# this script produces the pacheco results for Table 2: Comparison of our method with laternatives across all four topics.

import argparse
import pandas as pd


def combine_summaries(input_files, output_file):
    # List to store individual dataframes
    dfs = []

    # Read each input CSV file
    for file in input_files:
        df = pd.read_csv(file)
        dfs.append(df)

    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    # Group by 'dataset' and 'feature', then select the row with max 'f1_score'
    result = combined_df.loc[
        combined_df.groupby(["dataset", "feature"])["f1_score"].idxmax()
    ]

    # keep only the desired metrics
    cols = [
        "dataset",
        "feature",
        "precision",
        "recall",
        "f1_score",
        "roc_auc",
    ]
    result = result[cols]

    # melt the dataframe using ['group_id', 'dataset'] as id_vars
    result = result.melt(id_vars=["dataset", "feature"]).reset_index()

    # pivot the dataframe using ['dataset', 'variable'] as index
    result = result.pivot(
        index=["dataset", "variable"],
        columns="feature",
        values="value",
    ).reset_index()

    # sort as follows:
    # Define the desired order for 'dataset' and 'variable'
    dataset_order = ["xj", "hk", "debate", "blm"]
    variable_order = ["precision", "recall", "f1_score", "roc_auc"]

    # Convert 'dataset' and 'variable' columns to categorical with the specified order
    result["dataset"] = pd.Categorical(
        result["dataset"], categories=dataset_order, ordered=True
    )
    result["variable"] = pd.Categorical(
        result["variable"], categories=variable_order, ordered=True
    )

    # Sort the dataframe by 'dataset' and 'variable'
    result = result.sort_values(["dataset", "variable"])

    # sort the columns
    result = result[
        ["dataset", "variable", "hashtags", "selected_hashtags", "pacheco_cs3"]
    ]

    print(result)

    # Save the result to the output CSV file
    result.to_csv(output_file, index=False)


def main():
    parser = argparse.ArgumentParser(
        description="Combine summary CSV files and select rows with max f1_score."
    )
    parser.add_argument(
        "-i", "--input", nargs="+", required=True, help="Input CSV files"
    )
    parser.add_argument("-o", "--output", required=True, help="Output CSV file")

    args = parser.parse_args()

    combine_summaries(args.input, args.output)


if __name__ == "__main__":
    main()
