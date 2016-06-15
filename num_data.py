# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 14:34:47 2016

@author: Nate Plumb

This file contains a function created for the financial statements data from 
SEC filings found at 
https://www.sec.gov/dera/data/financial-statement-data-sets.html.
The function extracts data from the tag.txt file, organizes the data and 
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
    

# main function
def num_data():
    # extract data
    num = []
    with open("2016q1/num.txt", "r") as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            num.append(row)
            
    # print number of rows in data
    print "Rows in file:", len(num)
                
    # arrange data by column
    header = num[0]
    cols = []
    for col_idx in range(len(header)):
        data = []
        for row_idx in range(1, len(num)):
            data.append(num[row_idx][col_idx])
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
    
    # insert data into 'numbers' table
    count = 1   #len(num) - 1
    for row in range(5000):
        value, ddate, tag = orgd["value"][row], orgd["ddate"][row], orgd["tag"][row]
        qtrs, uom, adsh = orgd["qtrs"][row], orgd["uom"][row], orgd["adsh"][row]
        
        if value == "":
            value = "0.0000"
        
        footnote = orgd["footnote"][row]
        
        footnote_boo = str(footnote != "")
                
        cur.execute("SELECT sub_id FROM subs WHERE adsh = "+adsh+";")
        sub_id_num = str(cur.fetchone()[0])
        
        cur.execute("SELECT tag_id FROM tags WHERE tag = '"+tag+"';")
        tag_id_num = str(cur.fetchone()[0])
        
        sql = """INSERT INTO numbers (value, sub_id_num, tag_id_num, ddate, 
        qtrs, uom, footnote_boo) VALUES (
        """+value+", "+sub_id_num+", "+tag_id_num+", "+ddate+", "+qtrs+", '"+uom+"', "+footnote_boo+");"
        
        print "Entering row:", count

        cur.execute(sql)
        
        if footnote_boo == "True":
            cur.execute("SELECT LAST_INSERT_ID();")
            value_id_fn = str(cur.fetchone()[0])
            
            cur.execute("""INSERT INTO footnotes (value_id_fn, footnote) 
            VALUES ("""+value_id_fn+", '"+footnote+"');")
                    
        count += 1
    
    
    
    conn.commit()
    conn.close()
    
    
    
num_data()

