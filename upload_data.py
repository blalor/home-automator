#!/usr/bin/env python
# -*- coding: utf-8 -*-

## This script should (probably?) be run via cron, every X minutes (probably
## 1-5 or so).  It moves all data from the primary database, sensors.db, to
## the upload database, upload.db.  It then uploads the data to the remote
## server, where graphing, analysis, etc. is performed. This allows processes
## which are frequently inserting data into the database to do so unhindered.
## In the eventual case that the upload process either takes longer than a few
## seconds, or when the network connection is down, inserts into the primary
## database are unimpacted, and no data bound for the upstream server should
## be lost.

## upload.db has same schema for power and room_temp, but also has add'l
## upload_state table.
### create table upload_state (
###     table_name text not null,
###     prop_name text not null,
###     value text not null,
###     primary key (table_name, prop_name)
### );
### insert into upload_state values ('power', 'last_uploaded_timestamp', 0);
### insert into upload_state values ('room_temp', 'last_uploaded_timestamp', 0);
## use the "last_uploaded_timestamp" value to keep track of which records
## should get uploaded.  This'll make it possible to keep some data in the
## power table for real-time monitoring.

import os
import sqlite3
import logging

# http://www.hackorama.com/python/upload.shtml
import urllib2
import MultipartPostHandler

import cPickle as pickle
import tempfile
from datetime import datetime

upload_url = "http://example.com/upload_data"

auth_handler = urllib2.HTTPDigestAuthHandler()
auth_handler.add_password("<basic http auth realm>", upload_url, "<userid>", "<password>")

urlopener = urllib2.build_opener(MultipartPostHandler.MultipartPostHandler,
                                 auth_handler)

if __name__ == '__main__':
    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(name)s -- %(message)s',
                        filename='uploader.log',
                        filemode='a')
    
    log = logging.getLogger("uploader")
    
    log.info("starting up in %s" % (os.getcwd(),))
    
    conn = sqlite3.connect("upload.db", timeout = 30, isolation_level = "EXCLUSIVE")
    
    ## attach databases
    conn.execute("attach database 'sensors.db' as 'primary'")
    log.debug("'primary' attached")
    
    ## POWER =================================================================
    last_uploaded_power_rec = int(conn.execute("select value from 'main'.upload_state where table_name = 'power' and prop_name = 'last_uploaded_timestamp'").fetchone()[0])
    
    proceed_with_upload = True
    
    try:
        log.debug("moving 'power' from 'primary' to 'main'")
        
        ## insert rows into upload db
        conn.execute(
            """
            insert into 'main'.power
                select * from 'primary'.power
                 where 'primary'.power.ts_utc > ?
            """,
            (last_uploaded_power_rec,)
        )
        
        result = conn.execute("select max(ts_utc) from 'main'.power").fetchone()[0]
        if result != None:
            last_uploaded_power_rec = int(result)
        
        ## delete rows from primary table
        conn.execute(
            """delete from 'primary'.power where ts_utc < ?""",
            (last_uploaded_power_rec - (15 * 60),) # 15 minutes
        )
        
        ## update metadata table
        conn.execute(
            """
            update 'main'.upload_state
               set value = ?
             where table_name = 'power'
               and prop_name = 'last_uploaded_timestamp'
            """,
            (last_uploaded_power_rec,)
        )
        
        log.debug("done moving 'power'")
        
        conn.commit()
    except:
        proceed_with_upload = False

        log.critical("unable to migrate power data to upload.db", exc_info = True)
        conn.rollback()
    
    ## ROOM_TEMP =============================================================
    
    last_uploaded_temp_rec = int(conn.execute("select value from 'main'.upload_state where table_name = 'room_temp' and prop_name = 'last_uploaded_timestamp'").fetchone()[0])
    
    try:
        log.debug("moving 'room_temp' from 'primary' to 'main'")
        conn.execute(
            """
            insert into 'main'.room_temp
                select * from 'primary'.room_temp
                 where 'primary'.room_temp.ts_utc > ?
            """,
            (last_uploaded_temp_rec,)
        )
        
        result = conn.execute("select max(ts_utc) from 'main'.room_temp").fetchone()[0]
        if result != None:
            last_uploaded_temp_rec = int(result)
            
        conn.execute("delete from 'primary'.room_temp")
        
        ## update metadata table
        conn.execute(
            """
            update 'main'.upload_state
               set value = ?
             where table_name = 'room_temp'
               and prop_name = 'last_uploaded_timestamp'
            """,
            (last_uploaded_temp_rec,)
        )
        
        log.debug("done moving 'room_temp'")
        
        conn.commit()
    except:
        proceed_with_upload = False
        log.critical("unable to migrate room_temp data to upload.db", exc_info = True)
        conn.rollback()
    
    conn.execute("detach 'primary'")
    log.debug("'primary' detached")
    
    if proceed_with_upload:
        # data to upload
        upload_pkg = {}
        tmpf = tempfile.TemporaryFile()
        upload_successful = False
        
        ## start transaction, dump data to temp file
        try:
            result = []
            for row in conn.execute("select ts_utc, clamp1, clamp2 from 'main'.power").fetchall():
                result.append((datetime.fromtimestamp(row[0]), row[1], row[2]))
            
            if result:
                upload_pkg['power'] = result
                conn.execute("delete from 'main'.power")
            
            result = []
            for row in conn.execute("""
                select ts_utc, sophies_room, living_room, outside, basement,
                       master, office, master_zone, living_room_zone
                  from 'main'.room_temp
            """).fetchall():
                result.append((datetime.fromtimestamp(row[0]), row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))
            
            if result:
                upload_pkg['room_temp'] = result
                conn.execute("delete from 'main'.room_temp")
            
            if upload_pkg:
                # pickle the dict
                log.debug("pickling")
                pickle.dump(upload_pkg, tmpf)
                
                # seek to the beginning of the temp file so that it can be read by the
                # uploader
                tmpf.seek(0)
                
                ## now, upload the data
                log.debug("uploading")
                resp = urlopener.open(upload_url, {'pickle_file': tmpf})
                
                if resp.code == 200:
                    upload_successful = True
                else:
                    log.critical("FAILURE: %d -- %s" % (r.code, r.msg))
            else:
                log.info("no data to upload")
                upload_successful = True
        finally:
            tmpf.close()
            
            if upload_successful:
                log.debug("committing transaction")
                conn.commit()
            else:
                log.debug("rolling back transaction")
                conn.rollback()
