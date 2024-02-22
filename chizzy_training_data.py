import sqlite3
import pandas as pd

timeframes = ['2015-01']

def open_file(file_name, category, dataframe):
     with open("{}".format(file_name), 'a', encoding = 'utf-8') as f:
                for content in dataframe['{}'.format(category)].values:
                    f.write(str(content) + '\n') 
    

for timeframe in timeframes:
    connection = sqlite3.connect('{}.db'.format(timeframe))
    c = connection.cursor()
    limit = 5000 #how much we put into our pandas data frame, 5000 rows of data
    last_unix = 0 #stores our last timestamp; used make it so our curr unix is greater than our last
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit:#means that theres needs to be 0 rows left before we enter
        #pulling sql data in pandas dataframe
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        if not test_done:
            open_file("test.from", 'parent', df)
            open_file("test.to", 'comment', df)
            #open returns a file object
             
            test_done = True
        else : 
            open_file("train.from", 'parent', df)
            open_file("train.to", 'comment', df)
        
        counter += 1
        if counter % 20 == 0:
            print(counter * limit, 'rows comleted so far')




            



