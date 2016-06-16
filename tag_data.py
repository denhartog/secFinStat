# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 12:19:17 2016

@author: Nate Plumb

This file contains a function created for the financial statements data from 
SEC filings found at 
https://www.sec.gov/dera/data/financial-statement-data-sets.html.
The function extracts data from the tag.txt file, organizes the data and 
puts the data in the fs_db database.
"""

import csv
import MySQLdb


# helper functions
def clean(esc_str):
    esc_str = list(esc_str)
    new_str = []
    for char in esc_str:
        if char == "'":
            new_str.append("\\'")
        elif char != "\\":
            new_str.append(char)
    return ''.join(new_str)
    

def tag_data():
    # extract data
    tag = []
    with open("2016q1/tag.txt", "r") as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            tag.append(row)
            
    # print number of rows in data
    print "Rows in file:", len(tag)
                
    # arrange data by column
    header = tag[0]
    cols = []
    for col_idx in range(len(header)):
        data = []
        for row_idx in range(1, len(tag)):
            data.append(tag[row_idx][col_idx])
        cols.append(data)
    
    # organize data in dictionary
    orgd = {}
    idx = 0
    for field in header:
        orgd[field] = cols[idx]
        idx += 1
    
    # escape any "'" and remove extra backslashes
    for field in orgd:
        for idx in range(len(orgd[field])):
            orgd[field][idx] = clean(orgd[field][idx])
            
       
    # get mysql password
    f = open("pw.txt", "r")
    pw = f.read()
    f.close()
    
    # connect to mysql
    conn = MySQLdb.connect(host = "localhost", user = "root", passwd = pw, 
                           db = "fs_db")
    cur = conn.cursor()
    
    
    count = 1
    for row in range(len(tag) - 1):
        print "Entering row:", count
        
        # insert data into 'tag_version' table
        version = orgd["version"][row]
        
        cur.execute("""INSERT IGNORE INTO tag_version (version) VALUES (
        '"""+version+"');")
        
        # insert data into 'docs' table
        doc = orgd["doc"][row]
        if len(doc) > 512:
            cur.execute("""INSERT INTO docs (doc_long) VALUES (
            '"""+doc+"');")
        else:
            cur.execute("SELECT doc_id FROM docs WHERE doc_short = '"+doc+"';")
            try:
                cur.fetchone()[0]
            except TypeError:
                cur.execute("""INSERT INTO docs (doc_short) VALUES (
                '"""+doc+"');")

        # insert data into 'tags' table
        tag = orgd["tag"][row]
                
        cur.execute("""SELECT version_id FROM tag_version WHERE version = 
        '"""+version+"';")
        version_id_tags = str(cur.fetchone()[0])
        
        cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"""' AND 
        version_id_tags = """+version_id_tags+";")
        try:
            cur.fetchone()[0]
        except TypeError:
            abstract, tlabel = orgd["abstract"][row], orgd["tlabel"][row]
            custom = orgd["custom"][row]
            
            if len(doc) > 512: 
                fld = "doc_long"
            else:
                fld = "doc_short"
            cur.execute("SELECT doc_id FROM docs WHERE "+fld+" ='"+doc+"';")
            doc_id_tags = str(cur.fetchone()[0])
            
            cur.execute("""INSERT INTO tags (tag, version_id_tags, custom, 
            abstract, tlabel, doc_id_tags) VALUES (
            '"""+tag+"', "+version_id_tags+", "+custom+", "+abstract+", '"+tlabel+"', "+doc_id_tags+");")
                
    
        # insert data into 'not_abs_tag_info' table
        if int(abstract) == False:
            datatype = orgd["datatype"][row]
            iord, crdr = orgd["iord"][row], orgd["crdr"][row]
            
            cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"""' AND
            version_id_tags = """+version_id_tags+";")            
            tag_id_noabs = str(cur.fetchone()[0])
            
            cur.execute("""INSERT IGNORE INTO not_abs_tag_info (tag_id_noabs, 
            datatype, iord, crdr) VALUES (
            """+tag_id_noabs+", '"+datatype+"', '"+iord+"', '"+crdr+"');")
            
        if count % 5000 == 0:
            conn.commit()
            
        count += 1
            
    
    conn.commit()
    
    conn.close()
        
    
tag_data()