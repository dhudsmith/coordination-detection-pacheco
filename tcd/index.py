#!/usr/bin/env python3
import argparse
import pickle
import sys

import ujson as json
from tqdm import tqdm


def prepare_content(in_fp, tqdm_desc="parse user tweet", tqdm_total=None):
    user_tweets = dict()

    for line in tqdm(in_fp, desc=tqdm_desc, total=tqdm_total):
        try:
            twt = json.loads(line)
        except Exception as e:
            print(line)
            raise e

        user_id = twt['user_id']

        target = user_tweets.get(user_id, list())
        target.append(
            (
                twt['created_at'],
                twt['text']
            )
        )
        user_tweets[user_id] = target

    return user_tweets


def main(args):
    parser = argparse.ArgumentParser(
        description='parse raw tweet.json.gz files into user-indexed table'
    )

    parser.add_argument('-i', '--infile',
        action="store", dest="infile", type=str, required=True,
        help="path to input parquet file of edge table")
    parser.add_argument('-o', '--outfile',
        action="store", dest="outfile", type=str, required=True,
        help="path to output pickle file of tweet table")
    parser.add_argument('-n', '--numline',
        action="store", dest="numline", type=int, required=True,
        help="number of lines in the input file")

    args = parser.parse_args(args)
    infile = args.infile
    outfile = args.outfile
    numline = args.numline

    # read input
    with open(infile, 'rb') as in_fp:
        # do work
        user_tweets = prepare_content(in_fp, tqdm_total=numline)
    with open(outfile, 'wb') as out_fp:
        # write output
        pickle.dump(user_tweets, out_fp)



if __name__ == '__main__': main(sys.argv[1:])
