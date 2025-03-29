'''
Creation date: 17/Mar/2025
Objective: read the ndjson file, do the analysis & write report?

'''

import pandas as pd
import os
import time
from mpi4py import MPI
import dask.dataframe as d_data

# start to timing:
s_time = time.time()


'''
implement mpi and slice the file

'''
# getting information form mpi
mpi_world = MPI.COMM_WORLD
rank = mpi_world.Get_rank()
size = mpi_world.Get_size()


# file size and partition size
f_path = "mastodon-106k.ndjson"
f_size = os.path.getsize(f_path)
part_size = f_size//size
mul_coe = int((f_size//1e9)+2)    


# dividing the file based on the node number
# for large files, divide file into many pieces (less than 1gb)
part_file = d_data.read_json(f_path, blocksize=f_size)
part_file = part_file.repartition(npartitions=size*mul_coe)


# timing the partition process:
p_time = time.time()
print(f'Node rank:{rank}, the partition process takes: {p_time-s_time}\n')



'''
    Getting the data and store it in each node
'''
# setting up the new dataframe to store the required info
result_columns = ["username", "id", "Hour", "Date", "sentiment"]
result_df = pd.DataFrame(columns=result_columns)

# timing the process:
time_a = time.time()

# testing the output
for i in range(mul_coe):

    # timing to get the partition:
    par_before = time.time()
    part_file1 = part_file.get_partition(mul_coe*rank+i)
    par_after = time.time()
    print(f'Node rank:{rank}, get partition time: {par_after-par_before}\n')
    

    # compute the file:
    df = part_file1.compute()
    df = df["doc"]
    [rows] = df.shape

    # timing the loading process:
    c_time = time.time()
    print(f'Node rank:{rank}, loading dataframe takes: {c_time-par_after}\n')


    # how to convert the first column (a dict) to more columns
    # we are now parsing through all the lines of data column
    for i in range(rows):
        # information to get:
        #   1. post created time
        #   2. sentiment score
        #   3. account id
        #   4. username
        # some error might happen: eg. no sentiment score

        df2 = df.iloc[i]

        if not pd.isna(df2['sentiment']):

            # try a better way:
            df3 = {}
            df3["sentiment"] = df2['sentiment']
            df3["username"] = df2['account']['username']
            df3["id"] = df2['account']['id']

            num_time = time.strptime(df2['createdAt'][:19], "%Y-%m-%dT%H:%M:%S")
            df3["Hour"] = time.strftime("%H", num_time)
            df3["Date"] = time.strftime("%Y-%m-%d", num_time)

            df3 = pd.DataFrame.from_dict(df3, orient='index').transpose()
            result_df = pd.concat([result_df, df3], ignore_index = True)
            



# get the time for getting the information:
time_b = time.time()
print(f'Node rank:{rank}, getting information takes: {time_b-time_a}\n')





'''

only gather the dataframe when it is the node 0

'''
# try something here:
result_df_list = mpi_world.gather(result_df, root =0)
if result_df_list:

    # concatenate all the dataframe altogether
    result_df_end =pd.concat(result_df_list, axis = 0, ignore_index=True)
    print(result_df_end.head())
    

    # get the :
    #   1. 5 happiest hours
    #   2. 5 saddest hours
    #   3. 5 happiest users
    #   4. 5 saddest users

    # 5 happiest & saddest hours




    h_hour = result_df_end.groupby('Hour')['sentiment'].sum()
    h_hour = h_hour.sort_values(ascending=False)
    h_five = h_hour.head()
    s_five = h_hour.tail().sort_values(ascending = True)
    print(f'{h_five}\n')
    print(f'{s_five}\n\n--------------------LINE----------------------------\n')


    # 5 happiest & saddest people based on id
    h_people = result_df_end.groupby('id').agg({
        'sentiment':'sum', 'username': 'first'})
    h_people = h_people.sort_values(by='sentiment', ascending=False)
    h_five_ppl = h_people.head()
    s_five_ppl = h_people.tail().sort_values(by='sentiment',ascending = True)
    print(f'{h_five_ppl}\n')
    print(f'{s_five_ppl}\n\n')


    # 5 happiest & saddest people based on username 
    # (to avoid the same id have different username)
    h_people = result_df_end.groupby('username').agg({
        'sentiment':'sum', 'id': 'first'})
    h_people = h_people.sort_values(by='sentiment', ascending=False)
    h_five_ppl = h_people.head()
    s_five_ppl = h_people.tail().sort_values(by='sentiment',ascending = True)
    print(f'{h_five_ppl}\n')
    print(f'{s_five_ppl}\n\n')

    print(f'gathering info and summary takes: {time.time()-time_b}\n')


    # here we timed our program:
    print(f"---------The program has run for {time.time()-s_time}\n-----------")
