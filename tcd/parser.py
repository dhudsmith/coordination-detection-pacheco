#!/usr/bin/env python3
import argparse
import re
import sys
from collections import Counter, defaultdict
from os.path import join

import pandas as pd
import ujson as json
from tqdm import tqdm

SOURCE_REGEX = re.compile(r'<a href=(?:\\*)\"(.+)(?:\\*)\" rel=(?:\\*)\"(.*)(?:\\*)\">(.*)</a>')



def counters2edges(counter_dict, p1_col, p2_col, w_col):
    edge = pd.DataFrame.from_records([
        (uid, feature, count)
        for uid, feature_counter in counter_dict.items()
        for feature, count in feature_counter.items()
    ], columns=[p1_col, p2_col, w_col])
    return edge


def prepare_content(in_fp, tqdm_desc="parse raw content", tqdm_total=None):
    user_features = {
        feature : defaultdict(Counter) for feature in
            [
                'hashtags',
                'selected_hashtags'
            ]
    }

    for line in tqdm(in_fp, desc=tqdm_desc, total=tqdm_total):
        try:
            twt = json.loads(line)
        except Exception as e:
            print(line)
            raise e

        user_id = twt['user_id']

        # count up the number of times each hashtag is used
        for ht in twt['hashtags']:
            user_features['hashtags'][user_id][ht] += 1

        # count up the number of times each selected hashtag is used
        for sht in twt['selected_hashtags']:
            user_features['selected_hashtags'][user_id][sht] += 1

    return user_features


def main(args):
    parser = argparse.ArgumentParser(
        description='parse raw tweet.json.gz files into feature parquets'
    )

    parser.add_argument('-i', '--infile',
        action="store", dest="infile", type=str, required=True,
        help="path to input parquet file of edge table")
    parser.add_argument('-o', '--outdir',
        action="store", dest="outdir", type=str, required=True,
        help="path to output directory for output randomized edge tables")
    parser.add_argument('-n', '--numline',
        action="store", dest="numline", type=int, required=True,
        help="number of lines in the input file")
    parser.add_argument('--p1col',
        action="store", dest="p1col", type=str, required=True,
        help="column name of nodes in partite 1")
    parser.add_argument('--p2col',
        action="store", dest="p2col", type=str, required=True,
        help="column name of nodes in partite 2")
    parser.add_argument('--wcol',
        action="store", dest="wcol", type=str, required=True,
        help="column name of edge weights")

    args = parser.parse_args(args)
    infile = args.infile
    outdir = args.outdir
    numline = args.numline
    p1_col = args.p1col
    p2_col = args.p2col
    w_col = args.wcol

    # read input
    with open(infile, 'rb') as in_fp:
        # do work
        user_features = prepare_content(in_fp, tqdm_total=numline)

    # write output
    for feature, counter_dict in user_features.items():
        fname = "{}.edge.parquet".format(feature)
        output_name = join(outdir, fname)

        if len(counter_dict) == 0:
            with open(output_name, 'w') as f:
                pass

        edge_df = counters2edges(counter_dict, p1_col, p2_col, w_col)

        edge_df.to_parquet(output_name)



if __name__ == '__main__': 
    main(sys.argv[1:])
