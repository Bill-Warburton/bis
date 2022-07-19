# ______________________________________________________________________________________________

    # counts separations by comparing temp_shortfam_yyyymm _id to the next one
    # temp_shortfam_yyyymm _id made in find_fam_type_month.py
# __________________________________________________________________________________


import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys
from datetime import datetime


import timeit
start = timeit.default_timer()

cur.execute(""" 
    create table separations (like temp_shortfam_199012_id);
    alter table separations add column chg_spo integer default 0;
    alter table separations add column ym text ;                """)           
            
for yr in range(1990, 2017) :
    for mon in range(1, 13) :
        ym = str.strip(str(yr))+str(mon).zfill(2)
        fn = "temp_shortfam_"+str.strip(str(yr))+str(mon).zfill(2)+"_id"
        next_mon = mon + 1
        next_yr = yr
        if next_mon == 13:
            next_mon = 1
            next_yr = yr + 1
        
        fn_next = "temp_shortfam_"+str.strip(str(next_yr))+str(next_mon).zfill(2)+"_id"
        end = timeit.default_timer()

        if yr < 2016 or mon < 12:
            print(yr,mon, end - start)
            cur.execute("""

                insert into separations
                select a.*, 
                        case when coalesce(trim(b.spouseid),'') = '' then 0 else 1 end as chg_spo,{ym}
                from {fn} a, {fn_next} b
                where a.study_id = b.study_id
                and a.spouseid != b.spouseid 
                and coalesce(trim(a.spouseid),'') != ''
                
                
                    """.format(ym = ym,fn = fn,fn_next = fn_next)) 
        conn.commit()

