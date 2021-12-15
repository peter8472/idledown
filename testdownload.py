# This should relaly be part
# of the podcast repo

import boto3

import dl2
from pathlib import Path
import sqlite3
import time




if __name__ == "__main__":
    econtalk = dl2.Podcast()

    for fn in econtalk.files:
        ans= input("grab {}?".format(fn.key))
        if ans == 'y' or ans == 'yes':
            x=fn
            break
    
    # exit(0)
    x =  dl2.Download("przwy-podcast", x.key,
         "testrun4",chunksize=59000,sleep=2,maxcount=10)
    x.create_sqlite_table()
    while True:
        state = x.get_state()[0]
        if state == dl2.PAUSE:
            time.sleep(0.5)
           
        elif state == dl2.STOP:
            break
        elif state == dl2.RUN:
            if (x.downpart() == True): # done
                exit(0)

            time.sleep(x.sleep)
        
        else:
            print("unknown state: {}".format(state))
            break
           