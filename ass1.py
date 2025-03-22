'''
Creation date: 17/Mar/2025
Objective: read the ndjson file, do the analysis & write report?

'''

import pandas as pd
import os
import time


'''
implementation of mpi4py here:

'''






df = pd.read_json("mastodon-106k.ndjson", lines=True, orient="records")
[rows,column] = df.shape
result_df = pd.DataFrame()

# how to convert the first column (a dict) to more columns
# we are now parsing through all the lines of data column
for i in range(rows):
    # information to get:
    #   1. post created time
    #   2. sentiment score
    #   3. account id
    #   4. username
    # some error might happen: eg. no sentiment score

    df2 = df["doc"][i]

    if df2['sentiment'] != 0 or not pd.isna(df2['sentiment']):

      # try a better way:
      df3 = pd.DataFrame.from_dict(df2, orient='index').transpose()[['sentiment']]
      df3['username'] = df2['account']['username']
      df3['id'] = df2['account']['id']

      num_time = time.strptime(df2['createdAt'][:19], "%Y-%m-%dT%H:%M:%S")
      df3['Hour'] = time.strftime("%H", num_time)
      df3['Date'] = time.strftime("%Y-%m-%d", num_time)

      # df3 = pd.concat([df3, df3['account']['id'], df3['account']['username']])

      result_df = pd.concat([result_df, df3])


    # if df2['sentiment'] != 0:
    #   # sentiment score
    #   print(df2['sentiment'])

    #   # time of the post
    #   print(df2['createdAt'])

    #   # account ids and usernames
    #   print(f"id: {df2['account']['id']}, username: {df2['account']['username']}\n\n\n")

#print(result_df)
#print(len(set(result_df['id'])))

# get the :
#   1. 5 happiest hours
#   2. 5 saddest hours
#   3. 5 happiest users
#   4. 5 saddest users

# 5 happiest & saddest hours
h_hour = result_df.groupby('Hour')['sentiment'].sum()
h_hour = h_hour.sort_values(ascending=False)
h_five = h_hour.head()
s_five = h_hour.tail()
print(f'{h_five}\n')
print(f'{s_five}\n\n--------------------------LINE----------------------------\n')


# 5 happiest & saddest people
h_people = result_df.groupby('id')['sentiment'].sum()
h_people = h_people.sort_values(ascending=False)
h_five_ppl = h_people.head()
s_five_ppl = h_people.tail()
print(f'{h_five_ppl}\n')
print(f'{s_five_ppl}\n\n')


