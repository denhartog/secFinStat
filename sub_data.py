# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 07:57:21 2016

@author: Nate Plumb

This file contains a function created for the financial statements data from 
SEC filings found at 
https://www.sec.gov/dera/data/financial-statement-data-sets.html.
The function extracts data from the sub.txt file, organizes the data and 
puts it in the fs_db database.
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
    
def has_partial(aciks):
    flag = False
    aciks = aciks.split(" ")
    for cik in aciks:
        if cik == "PARTIAL":
            flag = True
    return flag
    
def check_range(year):
    if year == "":
        year = 1901
    elif int(year) < 1901 or int(year) > 2155:
        year = 1901
    return str(year)
    
def check_null_date(nnv):
    if nnv == "":
        nnv = "0000"
    return nnv

def remove_hy(adsh):
    adsh = re.sub("-", "", adsh)
    return adsh



def sub_data():
    # extract data          
    sub = []
    with open("sub.txt", "r") as f:
        reader = csv.reader(f, delimiter = "\t")
        for row in reader:
            sub.append(row)                
    
    # arrange data by column
    header = sub[0]
    cols = []
    for col_idx in range(len(header)):
        data = []
        for row_idx in range(1, len(sub)):
            data.append(sub[row_idx][col_idx])
        cols.append(data)
    
    # organize data in dictionary
    orgd = {}
    idx = 0
    for field in header:
        orgd[field] = cols[idx]
        idx += 1
    
    # remove any escape "'" and remove extra backslashes
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
    
    
    # insert form names not already in 'forms' table
    for form_type in orgd["form"]:
        cur.execute("INSERT IGNORE INTO forms (type) VALUES ('"+form_type+"');")
    conn.commit()
    
    
    # insert data into 'firms_current' table
    for row in range(len(sub) - 1):
        cik, name, sic = orgd["cik"][row], orgd["name"][row], orgd["sic"][row]
        countryinc, stprinc = orgd["countryinc"][row], orgd["stprinc"][row]
        
        # prepare for empty 'sic' observations        
        if sic == "":
            sic = "0"

        sql = """INSERT IGNORE INTO firms_current (cik, name, sic, countryinc, 
        stprinc) VALUES ("""+cik+", \""+name+"\", "+sic+", '"+countryinc+"', '"+stprinc+"');"        
        print sql        
        cur.execute(sql)
                                                
    # commit changes
    conn.commit()
 
    
    # insert data into 'firms_past' table
    for row in range(len(sub) - 1):
        if orgd["former"][row] != "":
            changed, former = orgd["changed"][row], orgd["former"][row]
            cik = orgd["cik"][row] 
            cur.execute("SELECT firm_id FROM firms_current WHERE cik = "+cik+";")
            firm_id = str(cur.fetchone()[0])
            sql = """INSERT IGNORE INTO firms_past (firm_id_fp, former, changed) 
            VALUES ("""+firm_id+", '"+former+"', "+changed+");"
            print sql
            cur.execute(sql)
            
    conn.commit()
    

    # insert data into 'subs' table
    for row in range(len(sub) - 1):
        detail, afs, wksi = orgd["detail"][row], orgd["afs"][row], orgd["wksi"][row]
        nciks, period, form = orgd["nciks"][row], orgd["period"][row], orgd["form"][row]
        fp, filed, cik = orgd["fp"][row], orgd["filed"][row], orgd["cik"][row]
        accepted, prevrpt = orgd["accepted"][row], orgd["prevrpt"][row]
        
        adsh = remove_hy(orgd["adsh"][row])
        
        fye = check_null_date(orgd["fye"][row])        
        
        fy = check_range(orgd["fy"][row])        
        
        aciks_partial = str(has_partial(orgd["aciks"][row]))
    
        cur.execute("SELECT firm_id FROM firms_current WHERE cik = "+cik+";")
        firm_id_subs = str(cur.fetchone()[0])
        
        cur.execute("SELECT form_id FROM forms WHERE type = '"+form+"';")
        form_id_subs = str(cur.fetchone()[0])
        
        sql = """INSERT IGNORE INTO subs (adsh, firm_id_subs, form_id_subs, 
        afs, wksi, fye, period, fy, fp, filed, accepted, prevrpt, detail, nciks, 
        aciks_partial) VALUES (
        """+adsh+", "+firm_id_subs+", "+form_id_subs+", '"+afs+"', "+wksi+", '"+fye+"', "+period+", "+fy+", '"+fp+"', "+filed+", '"+accepted+"', "+prevrpt+", "+detail+", "+nciks+", "+aciks_partial+");"
                
        cur.execute(sql)
        
    conn.commit()

    # insert data into 'aciks' table
    for row in range(len(sub) - 1):
        if orgd["nciks"][row] > 1:
            adsh = remove_hy(orgd["adsh"][row])
            cur.execute("SELECT sub_id FROM subs WHERE adsh = "+adsh+";")
            sub_id_aciks = str(cur.fetchone()[0])
            
            cik_parent = orgd["cik"][row]
            sql = "SELECT firm_id FROM firms_current WHERE cik = "+cik_parent+";"
            cur.execute(sql)
            firm_id_parent = str(cur.fetchone()[0])
            
            aciks = orgd["aciks"][row].split(" ")
            for cik in aciks:
                cik = str(cik)
                if cik != "":
                    while len(cik) < 10:
                        cik = "0"+cik
                    sql = "SELECT firm_id FROM firms_current WHERE cik = "+cik+";"
                    print sql
                    cur.execute(sql)
                    try:
                        firm_id_subsid = str(cur.fetchone()[0])
                        sql = """INSERT IGNORE INTO aciks (sub_id_aciks, 
                        firm_id_parent, firm_id_subsid) VALUES (
                        """+sub_id_aciks+", "+firm_id_parent+", "+firm_id_subsid+");"
                        print sql                    
                        cur.execute(sql)
                    except TypeError:
                        sql = """INSERT IGNORE INTO aciks (sub_id_aciks, 
                        firm_id_parent, cik_subsid) VALUES (
                        """+sub_id_aciks+", "+firm_id_parent+", "+cik+");"
                        print sql
                        cur.execute(sql)
                        
            
    conn.commit()
    conn.close()
    
                
    
    