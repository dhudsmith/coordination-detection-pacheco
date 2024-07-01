# coordination-detection

Python library to detect coordinated campaigns on social media

## Data

The input and output data are stored in `data/{folder}`.

Each folder should have the following data resource:

### `raw_tweets.jsonl`

This is a json-lines file containing the tweet data. Each row should have the following keys:

- `user_id`: The id string of the user. This is repeated for each tweet produced by the user.
- `created_at`: The UTM datetime string for the time when the tweet was created
- `hashtags`: A list of hashtags used in the tweet
- `selected_hashtags`: a list of hashtags used in the tweet that also were selected by the narrative feature selection process. These are a subset of hashtags.
- `text`: the raw tweet text. The hashtags are contained in the tweet.

For example, a line might look like

```
{"user_id": "user001", "created_at": "2024-06-25T09:30:15Z", "hashtags": ["#tech", "#AI", "#innovation"], "selected_hashtags": ["#AI", "#innovation"], "text": "Exploring the frontiers of #tech with groundbreaking #AI applications. The future of #innovation is here!"}
```

The script `create_jsonl.py` takes 