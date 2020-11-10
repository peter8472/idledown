# This should relaly be part
# of the podcast repo

import boto3

import dl2
from pathlib import Path
import sqlite3
import time


class Podcast():
    def __init__(self):

        self.s3 = boto3.resource("s3")
        self.bucket = self.s3.Bucket("przwy-podcast")

        self.files = self.bucket.objects.all()
        myfiles = [i for i in self.files]
        myfiles.sort(key=lambda x: x.last_modified, reverse=True)
        for i in myfiles[0:7]:
            print(i.key, i.last_modified)


if __name__ == "__main__":
    econtalk = Podcast()
    x =  dl2.Download("przwy-podcast", "Hitzthought.mp3",
         "testrun3",chunksize=5000,sleep=2,maxcount=10)
    x.create_sqlite_table()
    while True:
        state = x.get_state()[0]
        if state == dl2.PAUSE:
            time.sleep(0.5)
            
        elif state == dl2.STOP:
            break
        elif state == dl2.RUN:
            x.downpart()
        else:
            print("unknown state: {}".format(state))
            break
           