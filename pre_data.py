# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 10:37:26 2016

@author: Nate Plumb

This file contains a function created for the financial statements data from 
SEC filings found at 
https://www.sec.gov/dera/data/financial-statement-data-sets.html.
The function extracts data from the pre.txt file, organizes the data and 
puts the data in the fs_db database.
"""

import csv
import MySQLdb
import re


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

def remove_hy(adsh):
    adsh = re.sub("-", "", adsh)
    return adsh
    

def pre_data():
    # extract data
    pre = []
    with open("2016q1/pre.txt", "r") as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            pre.append(row)
            
    # print number of rows in data
    print "Rows in file:", len(pre)
                
    # arrange data by column
    header = pre[0]
    cols = []
    for col_idx in range(len(header)):
        data = []
        for row_idx in range(1, len(pre)):
            data.append(pre[row_idx][col_idx])
        cols.append(data)
    
    # organize data in dictionary
    orgd = {}
    idx = 0
    for field in header:
        orgd[field] = cols[idx]
        idx += 1
    
    # escape any "'", remove extra backslashes and remove hyphens from adsh
    for field in orgd:
        for idx in range(len(orgd[field])):
            if field == "adsh":
                orgd[field][idx] = remove_hy(orgd[field][idx])
            orgd[field][idx] = clean(orgd[field][idx])
            
       
    # get mysql password
    f = open("pw.txt", "r")
    pw = f.read()
    f.close()
    
    # connect to mysql
    conn = MySQLdb.connect(host = "localhost", user = "root", passwd = pw, 
                           db = "fs_db")
    cur = conn.cursor()
    
    # insert data into 'presentations' table
    count = 1
    for row in range(len(pre) - 1):
        report, line, adsh = orgd["report"][row], orgd["line"][row], orgd["adsh"][row]
        stmt, inpth, tag = orgd["stmt"][row], orgd["inpth"][row], orgd["tag"][row]
        plabel = orgd["plabel"][row]
        
        cur.execute("SELECT sub_id FROM subs WHERE adsh = "+adsh+";")
        sub_id_pres = str(cur.fetchone()[0])
        
        cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"';")
        tag_id_pres = str(cur.fetchone()[0])
        
        sql = """INSERT INTO presentations (sub_id_pres, report, line, stmt, 
        inpth, tag_id_pres, plabel) VALUES (
        """+sub_id_pres+", +"+report+", "+line+", '"+stmt+"', "+inpth+", "+tag_id_pres+", '"+plabel+"');"
        
        print "Entering row:", count
        
        cur.execute(sql)
        
        count += 1
        
    conn.commit()
    conn.close()
    
    
pre_data()

