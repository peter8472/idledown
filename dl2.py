import boto3
import shelve
from pathlib import Path
import sqlite3
import time

SHELFNAME = "dl.shelf"
CHUNKSIZE = 1
SQLITE = "running.sqlite"
RUN = "run"
PAUSE = 'pause'
STOP = 'stop'

def get_downloads():
    pass
class Download():
    def __init__(self, bucket,key,dirname, chunksize , 	sleep=0,maxcount=0):
        self.s3 = boto3.resource("s3")
        self.bucket = bucket
        self.key = key
        self.sleep = sleep
        self.maxcount =  maxcount
        self.chunksize = chunksize
        self.object = self.s3.Object(self.bucket, self.key)
        self.download_directory  = Path.home() / ".aws" / "downloads" / dirname
        self.filename = self.download_directory / Path(key).name
        if not self.download_directory.exists():
            self.start_download()
        else:
            print('{} essists'.format(self.download_directory))
        shelfname = self.download_directory / "dl.shelf"
    def start_download(self):
        
        self.download_directory.mkdir(parents=True)
        self.create_sqlite_table()
    def find_database(self):
        sqlitefile = self.download_directory / SQLITE
        self.conn = sqlite3.connect(str(sqlitefile))

    def create_sqlite_table(self):
        sqlitefile = self.download_directory / SQLITE
        self.conn = sqlite3.connect(str(sqlitefile))
        self.conn.execute("""CREATE TABLE if not exists runtable (
        filename TEXT,
        bucket TEXT,
        key TEXT,
		state TEXT, 
		chunksize INT,
		sleep DECIMAL,
		count INT,
		maxcount INT,
        filesize INT
		);""")
        self.conn.execute("delete from runtable;")
        self.conn.execute("""insert into runtable values (?, ?
              );""", 
        (self.filename,self.bucket,self.key, "run", self.chunksize))
        self.conn.commit()
        



    def downpart(self):
        fd = None
        size = 0
        if self.filename.exists():
            size = self.filename.stat().st_size
            print("exists, size {}".format(size))
            fd = self.filename.open("ab")
            
        else:
            print("does not exisst")
            fd = self.filename.open("wb")

        try:            
            download = self.object.get(Range="bytes={}-{}".format(size,size+self.chunksize))
            reader = download['Body']
            print (download['ContentRange'])
            afterslash = download['ContentRange'].split("/")[1]
            print("afterslash: {}".format(afterslash))
            print("size is {}".format(download['ContentLength']))
            fd.write(reader.read())
            return True
        except boto3.exceptions.botocore.client.ClientError as e:
            print(e)
            return False
    def serve_forever(self):
        while True:
            state = self.get_state()[0]
            if state == PAUSE:
                time.sleep(0.5)
                
            elif state == STOP:
                break
            elif state == RUN:
                x.downpart()
            else:
                print("unknown state: {}".format(state))
                break
           

    def get_state(self):
        'get the desired state'
        
        try:
            cur = self.conn.execute("SELECT state FROM runtable")
            ans = cur.fetchall()
            if len(ans) != 1:
                print("error: {} rows in database, expecting 1".format(len(ans)))
            else:
                return ans[0]
        except sqlite3.OperationalError as e:
            print("sqlite3 operation problem: {}".format(e))
            exit(1)

            
        rows = cur.fetchall()
        if len(rows) < 1:
            return False
        return rows[0]
        

if __name__ == "__main__":
    x =  Download("przwy-podcast", "Hitzthought.mp3",
         "testrun3",chunksize=5000,sleep=2,maxcount=10)
    x.create_sqlite_table()
    while True:
        state = x.get_state()[0]
        if state == PAUSE:
            time.sleep(0.5)
            
        elif state == STOP:
            break
        elif state == RUN:
            x.downpart()
        else:
            print("unknown state: {}".format(state))
            break
           