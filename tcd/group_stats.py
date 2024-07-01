import argparse
import json

import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score


def compute_statistics(groups, authors):
    # Compute your desired statistics here
    # For example, you can calculate the mean, median, or any other statistics

    # Replace the following lines with your own logic
    summary_stats = {
        "num_groups": len(groups),
        "num_accounts_per_group": [len(group) for group in groups],
        "precision": [],
        "recall": [],
        "sensitivity": [],
        "specificity": [],
        "f1_score": [],
        "roc_auc": []
    }

    # groups is a list of lists of author_ids
    # for each list in groups, add a column to the authors dataframe
    # the columns will have a zero if the author_id is not in the group
    # and a one if the author_id is in the group
    for i, group in enumerate(groups):
        authors[f"group_{i}"] = authors['author_id'].isin(group).astype(int)

    for i in range(len(groups)):
        group_name = f"group_{i}"
        group_labels = authors[group_name]
        group_predictions = authors['is_needle'].astype(int)
        
        precision = precision_score(group_labels, group_predictions)
        recall = recall_score(group_labels, group_predictions)
        sensitivity = recall
        specificity = (group_labels == 0).sum() / (group_labels == 0).count()
        f1 = f1_score(group_labels, group_predictions)
        roc_auc = roc_auc_score(group_labels, group_predictions)
        
        summary_stats["precision"].append(precision)
        summary_stats["recall"].append(recall)
        summary_stats["sensitivity"].append(sensitivity)
        summary_stats["specificity"].append(specificity)
        summary_stats["f1_score"].append(f1)
        summary_stats["roc_auc"].append(roc_auc)

    return summary_stats

def main(groups, authors, output):
    # Load the JSON file
    with open(groups, 'r') as f:
        groups = json.load(f)

    # Load the parquet file
    authors = pd.read_parquet(authors)

    # Compute the statistics
    summary_stats = compute_statistics(groups, authors)

    # Save the summary statistics as a JSON file
    with open(output, 'w') as f:
        json.dump(summary_stats, f, indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Compute summary statistics from a JSON file.')
    parser.add_argument('-g', '--groups', type=str, required=True, 
                        help='Path to the input JSON file that lists accounts in each group')
    parser.add_argument('-a', '--authors', type=str, required=True,
                        help='Path to the input parquet file that lists author_id and is_needle')
    parser.add_argument('-o', '--output', type=str, required=True, 
                        help='Path to the output JSON file')
    args = parser.parse_args()

    main(args.groups, args.authors, args.output)
