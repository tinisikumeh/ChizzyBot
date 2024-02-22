#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sqlite3
import json #allows us to read data format
from datetime import datetime #used to ouput where we are when logging

timeframe = '2015-01'
sql_transaction = [] 

connection = sqlite3.connect('{}.db'.format(timeframe)) #format makes it so our database is named after the year & month
c = connection.cursor()#allows us to traverse through the rows of a set

#this is where we create our query
def create_table():
    #execute parses the string in SQL and creates a table if it doesn't exist already called parent_reply
    #TEXT is the datatype of parent_reply
    #PRIMARY KEY constraint uniquely identifies each record in a table
    #Primary keys must contain UNIQUE values, and cannot contain NULL values.
    #UNIQUE is a constraint that ensures that all values in a column are unique
    
    m = """CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, 
    comment_id TEXT UNIQUE, parent TEXT,  comment TEXT, subreddit TEXT, unix INT, score INT)"""
    c.execute(m)

def format_data(data):
    data = data.replace("\n"," newlinechar").replace("\r"," newlinechar").replace('"',"'")
    return data

def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()# reutrns a single record
        #print(result)
       
        if result != None:
            
            return result[0]
        else:
            #print ("1")
            return False
    except Exception as e:
        return False
        
def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True
    

def find_parent(pid):
    try:
        #looking for any place where the comment_id is the initial parent text
        #we do this as we know every string has a parent_id, but this doesn't mean it has the parent text
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()# reutrns a single record
        #print(result[0])
        if result != None:
            return result[0]
        else: 
            return False
    except Exception as e:
        return False
        print("find_parent", e)
    
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        #indicates the beginnig of a series of sql statements
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                print("done")
                c.execute(s)
                connecton.commit()
            except:
                pass
        #must call .commit() after every transaction that modifies data
        #connecton.commit()
    
        sql_transaction = []
        print("database complete")
    
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
        #print("1")
    except Exception as e:
        print('s-UPDAT insertion',str(e))
        
def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    #INSERTING A NEW ROW
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
        #print("1")
    except Exception as e:
        print('s-PARENT insertion',str(e))


def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    #WE INSERT THIS COMMENT SO THE CHILDREN OF IT HAVE PARENT INFORMATION
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
        #print("1")
    except Exception as e:
        print('s-NO_PARENT insertion',str(e))

        
        
        
if __name__ == "__main__":
    create_table()
    row_counter = 0
    paired_rows = 0 #counts how many parent-child pairs we've come up with
    
    #buffer is the amount of data we can temporarily hold in memory before processing it
    with open("/Users/tinisikumeh/aiChatbot/RC_{}".format(timeframe, buffering=1000)) as f:
        for row in f:
            row_counter += 1
            #converts Json data  row into a python object in the form of a dictionary
            row = json.loads(row)
            parent_id = row['parent_id']
            body = format_data(row['body'])
            comment_id = row['name']
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            start_row = 1
            cleanup = 1000
            
            
            parent_data = find_parent(parent_id)
            #if statement means that someone saw a comment and upvoted it hence score > 0
            #the function is looking to see if theres's and existing reply to a parent_id
            #if there's an existing reply, we want to know whether or not it has a better score
            if score >= 2:
                    if row_counter > start_row:
                        if row_counter % cleanup == 0:
                            print("Cleaning up!")
                            print("Turning journal off temporarily...")
                            c.execute("PRAGMA journal_mode = OFF")
                            print("Deleting null parent rows...")
                            sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                            c.execute(sql)
                            print("committing changes")
                            connection.commit()
                            print("vacuuming...")
                            c.execute("VACUUM")
                            print("committing...")
                            connection.commit()
                            print("Turning journal back on (\"MEMORY\" mode)...")
                            c.execute("PRAGMA journal_mode = MEMORY")
                            connection.commit()
                            print("Back to loading and processing new rows!\n")
                
                    existing_comment_score = find_existing_score(parent_id)
                    #print( parent_data)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            if acceptable(body):
                            #body is  the new comment
                            #updates our existing comment as we have a higher score available
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if acceptable(body):
                            #if we have parent with data
                            if parent_data:
                                #print("1")
                                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                paired_rows +=1
                            else:
                                #if we have a top level comment there will be no parent so the thread itself is the parent
                                #this comment might still be a parent to some other comment, whos data we want
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)
            
            if row_counter % 1000 == 0:
                print("Total rows read: {}, Paired rows: {}, Time: {}".format(row_counter, paired_rows,str(datetime.now())))

                        

                
                
            
    
    
    
    
    
    
    
    
    
    
    
        
        
        
        
        
        
        


# In[ ]:





# In[ ]:





# In[ ]:




