'''
Creation date: 17/Mar/2025
Objective: read the ndjson file and do the analysis

'''

import pandas as pd

df = pd.read_json("mastodon-106k.ndjson", lines=True, orient="records")
print(type(df["doc"][0]))
[rows,column] = df.shape

# how to convert the first column (a dict) to more columns
for i in range(rows):
    # we get the sentiment score
    print(df["doc"][i]['sentiment'])

