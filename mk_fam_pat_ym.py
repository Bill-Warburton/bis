# ______________________________________________________________________________________________

    # make a pattern of family types
# __________________________________________________________________________________


import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys
from datetime import datetime


import timeit
start = timeit.default_timer()



for yr in range(1991, 2017) :
    fnyr = "rpb_fam_pat_ym_"+str.strip(str(yr))
    cur.execute("""
                drop table if exists temp_fam;
                drop table if exists {fnyr};
                create table {fnyr} (studyid text, pattern text); 
                create table temp_fam (studyid text, ym int, famtype text); 
                """.format(fnyr = fnyr)) 
    conn.commit()

    for mon in range(1, 13) :
        tempyr = 'temp_'+str.strip(str(yr))
        fn = "temp_shortfam_"+str.strip(str(yr))+str(mon).zfill(2)+"_id"
        ym = mon - 1
        end = timeit.default_timer()

        print(yr,ym, end - start)
        cur.execute("""
            insert into temp_fam 
            select studyid, {ym} as ym,
                case  when famtype = '2p' then  '1'
                when famtype = 'sna' then '2'
                when famtype = 'c' then '3'
                when famtype = '1p'  then '4'
                when famtype = 'sny' then '5'
                else '6' end as famtype
                from {fn};
                """.format(ym = ym,fn = fn)) 


    t_out = cur.execute("""
                select * from temp_fam order by studyid;
                """)

    all_involv = cur.fetchall()

    blankrecord = '0'*12
    t_id  = '0'*9
    start = timeit.default_timer()

    for row in all_involv:
        pos2 = row[1]
        if t_id != row[0]:
            if t_id  != '0'*9 :
                cur.execute("""
                    insert into {fnyr} values('{t_id}', '{outrecord}');
                    """.format(t_id= t_id, outrecord=outrecord, fnyr = fnyr))
            t_id = row[0]
            outrecord = blankrecord
        tfam = row[2]    
        t_out = outrecord[:pos2] + tfam + outrecord[pos2+1:]
        outrecord = t_out
    cur.execute("""
        insert into {fnyr} values('{t_id}', '{outrecord}');
        """.format(t_id= t_id, outrecord=outrecord, fnyr = fnyr))

    conn.commit()    
    end = timeit.default_timer()
    print(end - start, "I got here 1 ")

cur.execute("""
            drop table if exists rpb_fam_pat_ym;
            create table rpb_fam_pat_ym as
            select * from rpb_fam_pat_ym_1991; 
            """) 
conn.commit()


for yr in range(1992, 2016) :
    fnyr = "rpb_fam_pat_ym_"+str.strip(str(yr))
    tpos = (yr - 1991)*12
    tfill = '0'*tpos
    end = timeit.default_timer()
    print(yr, end - start)
    cur.execute("""
        drop table if exists temp;
        create table temp as 
        select coalesce(a.studyid, b.studyid) as studyid,
        coalesce(substring(a.pattern from 1 for {tpos}), '{tfill}')
        || coalesce(b.pattern, '000000000000') as pattern
        from rpb_fam_pat_ym a
        full outer join {fnyr} b
        on a.studyid = b.studyid;
        
        drop table if exists rpb_fam_pat_ym;
        create table rpb_fam_pat_ym as
        select * from temp; 
        """.format(fnyr = fnyr,tpos = tpos, tfill = tfill)) 
    conn.commit()

cur.execute("""
            drop table if exists rpb_fam_pat_ym_bdt;
            create table rpb_fam_pat_ym_bdt as
            select a.*, sex, dobyyyymm  
            from rpb_fam_pat_ym a,
            demographics_unduped b
            where a.studyid = b.studyid; 
            """) 
conn.commit()

with open('r:\\working\\users\\bill\\divorce\\rpb_fam_pat_ym_bdt.csv', 'w') as f:
    cur.copy_expert('copy rpb_fam_pat_ym_bdt  to stdout with (format csv, header)',f)
