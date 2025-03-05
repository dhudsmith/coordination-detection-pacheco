import argparse
import json
import csv

import pandas as pd


def summarize_stats(input_files, output_file):
    stats_ls = []
    for f in input_files:
        # get dataset name
        dataset_name = f.split("/")[-4]
        feature_name = f.split("/")[-3]
        interaction_percent = f.split("/")[-2]
        # load stats
        with open(f, "r") as file:
            stats = json.load(file)

        for ix in range(stats["num_groups"]):
            group_dict = {"group_id": ix}
            group_dict["dataset"] = dataset_name
            group_dict["feature"] = feature_name
            group_dict["interaction_percent"] = interaction_percent
            for key in [
                "precision",
                "recall",
                "sensitivity",
                "specificity",
                "f1_score",
                "roc_auc",
            ]:
                group_dict[key] = stats[key][ix]

            stats_ls.append(group_dict)

    df = pd.DataFrame(stats_ls)

    try:
        df = df.sort_values(
            by=["dataset", "feature", "interaction_percent", "group_id"]
        )
    except KeyError:
        print(
            "WARNING: Could not sort by dataset, feature, interaction_percent, group_id. This is likely the result of having no groups present from graph clustering."
        )
        pass

    print(df)

    df.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calculate group statistics")
    parser.add_argument("-i", "--group-stats", nargs="+", help="List of input files")
    parser.add_argument("-o", "--output", help="Output file")

    args = parser.parse_args()

    summarize_stats(args.group_stats, args.output)
