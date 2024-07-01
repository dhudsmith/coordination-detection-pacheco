import argparse

import pandas as pd


def clean_authors(input_file, output_file):
    # Load input csv as pandas dataframe
    df = pd.read_csv(input_file)

    if 'handle' in df.columns:
        # rename handle to author_id
        df.rename(columns={'handle': 'author_id'}, inplace=True)

    # only keep handle and is_needle columns
    df = df[['author_id', 'is_needle']]

    # drop missing author id or is needle indicators
    df = df.dropna(subset=['author_id', 'is_needle'])

    # convert all values to strings
    df.is_needle = df.is_needle.astype(int)  # in case in bool format
    df = df.astype(str)

    # Save standardized dataframe as parquet file
    df.to_parquet(output_file, index=False)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Clean authors data')
    parser.add_argument('-i', '--input', help='Input CSV file')
    parser.add_argument('-o', '--output', help='Output parquet file')
    args = parser.parse_args()

    clean_authors(args.input, args.output)