import os
import argparse

import pandas as pd


def create_edges(hashtag_counts, top_hashtags, outdir, p1_col, p2_col, w_col, num_top_features):
    possible_count_cols = [
        ['author_id', 'hashtags', 'successes'],
        ['handle', 'hashtags', 'successes'],
    ]

    top_hashtag_cols = ['hashtags', 'kld']

    df_counts = pd.read_csv(hashtag_counts)
    df_top = pd.read_csv(top_hashtags)

    assert df_counts.columns.tolist() in possible_count_cols, f"Columns must be one of {possible_count_cols}"
    assert df_top.columns.tolist() == top_hashtag_cols, f"Columns must be {top_hashtag_cols}"

    # rename the columns
    df_counts.columns = [p1_col, p2_col, w_col]
    df_top.columns = [p2_col, 'kld']

    # create the selected feature dataframe by filtering to the top 40 hashtags
    df_top = df_top.sort_values(by='kld', ascending=False)
    top_hashtags = df_top[p2_col][:num_top_features]
    df_counts_selected = df_counts[df_counts[p2_col].isin(top_hashtags)]

    # create the "five of the same features" dataframe
    
    # write the original and selected files to parquet
    os.makedirs(outdir + "/hashtags", exist_ok=True)
    os.makedirs(outdir + "/selected_hashtags", exist_ok=True)
    df_counts.to_parquet(outdir + "/hashtags/edge.parquet")
    df_counts_selected.to_parquet(outdir + "/selected_hashtags/edge.parquet")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create edges script")
    parser.add_argument("--hashtag_counts", required=True, help="Path to hashtag counts file")
    parser.add_argument("--top-hashtags", required=True, help="Path to top hashtags file")
    parser.add_argument("-o", "--outdir", required=True, help="Output directory")
    parser.add_argument("--p1col", required=True, help="Column name for p1")
    parser.add_argument("--p2col", required=True, help="Column name for p2")
    parser.add_argument("--wcol", required=True, help="Column name for weight")
    parser.add_argument("--num-top-features", type=int, required=True, help="Number of top features to use")
    args = parser.parse_args()

    create_edges(
        args.hashtag_counts, 
        args.top_hashtags, 
        args.outdir, 
        args.p1col, 
        args.p2col, 
        args.wcol, 
        args.num_top_features
    )