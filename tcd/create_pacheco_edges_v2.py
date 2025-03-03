import pandas as pd
import argparse
from ast import literal_eval

# support filter: per account at least 5 tweets per day and five unique hashtags


def process_tweets(input_file, output, min_num_tweets, min_num_hashtags):
    # Read the CSV file
    df = pd.read_csv(input_file, engine="python")

    # keep only the columns we need
    df = df[["author_id", "created_at", "is_needle", "hashtags"]]

    # Convert 'created_at' to datetime
    initial_count = len(df)
    df["created_at"] = pd.to_datetime(
        df["created_at"], format="ISO8601", errors="coerce"
    )

    df = df.dropna(subset=["created_at"])
    dropped_count = initial_count - len(df)
    print(f"Dropped/Kept rows: {dropped_count}/{len(df)}")
    if dropped_count > 0:
        print(f"WARNING: dropped {dropped_count} rows due to missing created_at values")
        print("Needle status among dropped rows:")
        print(df[df["created_at"].isna()].is_needle.value_counts())

        print("Needle status among non-dropped rows:")
        print(df[~df["created_at"].isna()].is_needle.value_counts())

    df.drop(columns=["is_needle"], inplace=True)

    # extract date
    df["date"] = df["created_at"].dt.date
    df = df.sort_values(by="created_at")

    #
    # keep (account, date) pairs with at least the minimum number of tweets
    #

    df_groups = (
        df[["author_id", "date"]].groupby(["author_id", "date"]).size()
    ).reset_index(name="size")

    df_groups = df_groups[df_groups["size"] >= min_num_tweets]
    df_groups.drop(columns=["size"], inplace=True)
    df = df.merge(df_groups, on=["author_id", "date"], how="inner")

    #
    # create hashtag usage features
    # in this version we use the set of unique hashtags per (account, date) pair
    #

    #  first, convert string representation of hashtags to list, handling potential errors
    def eval_lists(x):
        try:
            ls = literal_eval(x)

            # filter out empty strings from list
            ls = [s for s in ls if s not in {"", " ", None}]

            return ls
        except Exception:
            return []

    df["hashtags"] = df["hashtags"].apply(eval_lists)

    # drop the rows with empty hashtag lists. These do not contribute
    # to the hashtag set feature
    df = (
        df[df["hashtags"].apply(lambda x: len(x) > 0)]
        .sort_values(by=["author_id", "date"])
        .reset_index()
    )

    # now, aggregate the hashtag lists for each (account, date) pair
    # the resulting list is the set of unique hashtags shared by the account in a day
    def hashtag_set(d):
        hts = set()
        for i, row in d.iterrows():
            hts.update(row["hashtags"])

        return pd.Series(
            {
                "hashtags": list(hts),
            }
        )

    df = pd.DataFrame(
        df.groupby(["author_id", "date"], as_index=False).apply(
            hashtag_set, include_groups=False
        )
    )

    #
    # keep (account, date) pairs with at least the minimum number of hashtags
    #
    df["num_hashtags"] = df["hashtags"].apply(lambda x: len(x))
    df = df[df["num_hashtags"] >= min_num_hashtags].reset_index(drop=True)
    df.drop(columns=["num_hashtags"], inplace=True)

    #
    # create the feature data based on date and hashtag sequence
    #

    # create a string representation of the hashtag list
    df["hashtags"] = df["hashtags"].apply(lambda x: "-".join(x))

    # combine the date and hashtag columns with an underscore
    # this is the feature column for pacheo case study 3
    df["feature"] = df["date"].astype(str) + "_" + df["hashtags"]

    # drop features that only appear once
    # appearing once implies that only a single account has the feature
    feature_counts = df["feature"].value_counts()
    df = df[df["feature"].isin(feature_counts[feature_counts > 1].index)]

    # structure to uid, feature, cnt for compatibility with pipeline
    df = df[["author_id", "feature"]]
    df["cnt"] = 1

    # rename author_id to uid
    df.rename(columns={"author_id": "uid"}, inplace=True)

    # sort by feature column
    df = df.sort_values(by=["feature", "uid"])

    # save

    df.to_parquet(output)
    df.to_csv(output.replace(".parquet", ".csv"), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process tweets and create edge files")
    parser.add_argument("-i", "--input", required=True, help="Input tweets CSV file")
    parser.add_argument("-o", "--output", required=True, help="Output parquet file")
    parser.add_argument(
        "--min-num-tweets",
        type=int,
        default=3,
        help="Minimum number of tweets per (account, day)",
    )
    parser.add_argument(
        "--min-num-hashtags",
        type=int,
        default=4,
        help="Minimum number of hashtags per (account, day)",
    )

    args = parser.parse_args()

    process_tweets(args.input, args.output, args.min_num_tweets, args.min_num_hashtags)
