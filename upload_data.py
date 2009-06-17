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

import sqlite3
import traceback

# http://www.hackorama.com/python/upload.shtml
import urllib2
import MultipartPostHandler

import cPickle as pickle
import tempfile

upload_url = "http://example.com/upload_data"

auth_handler = urllib2.HTTPDigestAuthHandler()
auth_handler.add_password("<basic http auth realm>", upload_url, "<userid>", "<password>")

urlopener = urllib2.build_opener(MultipartPostHandler.MultipartPostHandler,
                                 auth_handler)

if __name__ == '__main__':
    conn = sqlite3.connect(":memory:", timeout = 30, isolation_level = "EXCLUSIVE")
    
    ## attach databases
    conn.execute("attach database 'sensors.db' as 'primary'")
    print "'primary' attached"
    
    conn.execute("attach database 'upload.db' as 'upload'")
    print "'upload' attached"
    
    ## POWER =================================================================
    last_uploaded_power_rec = int(conn.execute("select value from 'upload'.upload_state where table_name = 'power' and prop_name = 'last_uploaded_timestamp'").fetchone()[0])
    
    proceed_with_upload = True
    
    try:
        print "moving 'power' from 'primary' to 'upload'"
        
        ## insert rows into upload table
        conn.execute(
            """
            insert into 'upload'.power
                select * from 'primary'.power
                 where 'primary'.power.ts_utc > ?
            """,
            (last_uploaded_power_rec,)
        )
        
        result = conn.execute("select max(ts_utc) from 'upload'.power").fetchone()[0]
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
            update 'upload'.upload_state
               set value = ?
             where table_name = 'power'
               and prop_name = 'last_uploaded_timestamp'
            """,
            (last_uploaded_power_rec,)
        )
        
        print "done moving 'power'"
        
        conn.commit()
    except:
        proceed_with_upload = False
        conn.rollback()
        traceback.print_exc()
    
    ## ROOM_TEMP =============================================================
    
    last_uploaded_temp_rec = int(conn.execute("select value from 'upload'.upload_state where table_name = 'room_temp' and prop_name = 'last_uploaded_timestamp'").fetchone()[0])
    
    try:
        print "moving 'room_temp' from 'primary' to 'upload'"
        conn.execute(
            """
            insert into 'upload'.room_temp
                select * from 'primary'.room_temp
                 where 'primary'.room_temp.ts_utc > ?
            """,
            (last_uploaded_temp_rec,)
        )
        
        result = conn.execute("select max(ts_utc) from 'upload'.room_temp").fetchone()[0]
        if result != None:
            last_uploaded_temp_rec = int(result)
            
        conn.execute("delete from 'primary'.room_temp")
        
        ## update metadata table
        conn.execute(
            """
            update 'upload'.upload_state
               set value = ?
             where table_name = 'room_temp'
               and prop_name = 'last_uploaded_timestamp'
            """,
            (last_uploaded_temp_rec,)
        )
        
        print "done moving 'room_temp'"
        
        conn.commit()
    except:
        proceed_with_upload = False
        conn.rollback()
        traceback.print_exc()
    
    conn.execute("detach 'primary'")
    print "'primary' detached"
    
    if proceed_with_upload:
        # data to upload
        upload_pkg = {}
        tmpf = tempfile.TemporaryFile()
        upload_successful = False
        
        ## start transaction, dump data to temp file
        try:
            result = conn.execute("select ts_utc, clamp1, clamp2 from 'upload'.power").fetchall()
            
            if result:
                upload_pkg['power'] = result
                conn.execute("delete from 'upload'.power")
            
            result = conn.execute("""
                select ts_utc, sophies_room, living_room, outside, basement,
                       master, office, master_zone, living_room_zone
                  from 'upload'.room_temp
            """).fetchall()
            
            if result:
                upload_pkg['room_temp'] = result
                conn.execute("delete from 'upload'.room_temp")
            
            if upload_pkg:
                # pickle the dict
                print "pickling"
                pickle.dump(upload_pkg, tmpf)
                
                # seek to the beginning of the temp file so that it can be read by the
                # uploader
                tmpf.seek(0)
                
                ## now, upload the data
                print "uploading"
                resp = urlopener.open(upload_url, {'pickle_file': tmpf})
                
                if resp.code == 200:
                    upload_successful = True
                else:
                    print "FAILURE: %d -- %s" % (r.code, r.msg)
            else:
                print "no data to upload"
                upload_successful = True
        finally:
            tmpf.close()
            
            if upload_successful:
                print "committing transaction"
                conn.commit()
            else:
                print "rolling back transaction"
                conn.rollback()
