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
    def __init__(self, bucket,key,dirname, chunksize = CHUNKSIZE):
        self.s3 = boto3.resource("s3")
        self.bucket = bucket
        self.key = key
        self.chunksize = chunksize
        self.object = self.s3.Object(self.bucket, self.key)
        self.download_directory  = Path.home() / ".aws" / "downloads" / dirname
        self.filename = self.download_directory / Path(key).name
        if not self.download_directory.exists():
            self.start_download(dirname)
        else:
            print('{} essists'.format(self.download_directory))
        shelfname = self.download_directory / "dl.shelf"
    def start_download(self):
        
        self.download_directory.mkdir(parents=True)
        self.create_sqlite_table()
    def create_sqlite_table(self):
        sqlitefile = self.download_directory / SQLITE
        conn = sqlite3.connect(str(sqlitefile))
        conn.execute("CREATE TABLE if not exists runtable (var TEXT PRIMARY KEY, val TEXT);")
        conn.execute("delete from runtable;")
        conn.execute("insert into runtable values (?, ?);", ("state", RUN))
        conn.commit()
        conn.close()



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
            print("size is {}".format(download['ContentLength']))
            fd.write(reader.read())
            return True
        except boto3.exceptions.botocore.client.ClientError as e:
            print(e)
            return False
    def get_state(self):
        'get the desired state'
        sqlitefile = self.download_directory / SQLITE
        conn = sqlite3.connect(str(sqlitefile))
        try:
            cur = conn.execute("SELECT val FROM runtable where var == 'state'")
        except sqlite3.OperationalError as e:
            conn.close()
            self.create_sqlite_table()
            return RUN

            
        rows = cur.fetchall()
        if len(rows) < 1:
            return False
        return rows[0]



            
        
        

if __name__ == "__main__":
    # start_download("przwy-podcast", "mytabs/tabs1.json", "tabs")
    # x =  Download("przwy-podcast", "mytabs/tabs1.json", "tabs3",chunksize=500)
    x =  Download("przwy-podcast", "android-studio-ide-193.6626763-windows.exe",
         "testrun2",chunksize=5000000)
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
           