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
    with open("tag.txt", "r") as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            tag.append(row)
                
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
    fname = open("pw.txt", "r")
    pw = fname.read()
    
    # connect to mysql
    conn = MySQLdb.connect(host = "localhost", user = "root", passwd = pw, 
                           db = "fs_db")
    cur = conn.cursor()
    
    
    # insert data into 'tag' table
    for row in range(len(tag) - 1):
        tag, custom = orgd["tag"][row], orgd["custom"][row]
        abstract, tlabel = orgd["abstract"][row], orgd["tlabel"][row]
        
        cur.execute("""INSERT IGNORE INTO tags (tag, custom, abstract, tlabel) 
        VALUES ('"""+tag+"', "+custom+", "+abstract+", '"+tlabel+"');")
        
    conn.commit()
    
    # insert data into 'not_abs_tag_info' table
    for row in range(len(tag) - 1):
        abstract = orgd["abstract"][row]
        if int(abstract) == False:
            tag, datatype = orgd["tag"][row], orgd["datatype"][row]
            iord, crdr = orgd["iord"][row], orgd["crdr"][row]
            
            cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"';")            
            tag_id_noabs = str(cur.fetchone()[0])
            
            cur.execute("""INSERT IGNORE INTO not_abs_tag_info (tag_id_noabs, 
            datatype, iord, crdr) VALUES (
            """+tag_id_noabs+", '"+datatype+"', '"+iord+"', '"+crdr+"');")
            
    conn.commit()
    
    # insert data into 'stand_tag_ver' table
    for row in range(len(tag) - 1):
        custom = orgd["custom"][row]
        if int(custom) == False:
            version = orgd["version"][row]
            
            tag = orgd["tag"][row]
            cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"';")
            tag_id_stand = str(cur.fetchone()[0])
            
            cur.execute("""INSERT IGNORE INTO stand_tag_ver (tag_id_stand, 
            version) VALUES (
            """+tag_id_stand+", '"+version+"');")
    
    conn.commit()
    
    conn.close()
        
    
