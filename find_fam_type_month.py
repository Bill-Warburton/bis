
# ______________________________________________________________________________________________

			# This program creates files temp_shortfam_yyyymm_id
            # for months 198601 to 201812
            # The variables are 
                # study_id
                # famtype
                # fam (contract numer)
                # spouseid
                
            
# ______________________________________________________________________________________________
	

import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys

import timeit
start = timeit.default_timer()


for yr in range(1986, 2019) :
    for mon in range(1, 13) :
        tempyr = 'temp_'+str.strip(str(yr))
        fn = "temp_shortfam_"+str.strip(str(yr))+str(mon).zfill(2)+"_id"
        print(fn)
        ref_date =  str.strip(str(yr)) + "-" + str(mon).zfill(2)+ "-15"
        cur.execute("""
                    /*  find all people in R&PB on ref_date*/
                    /*  take the non-fixed records first*/
                    drop table if exists samp4;
                    create table samp4 as
                    select study_id, max(rpb_contract_fix_cancel_contract_number) as fam,
                    to_date('{ref_date}', 'YYYY-MM-DD') as change_date,
                    max(demographics_unduped_dob) as demographics_unduped_dob,
                    max(demographics_unduped_gender) as demographics_unduped_gender
                    from rpb_contract_fix_cancel_dob
                    where rpb_contract_fix_cancel_fixed = 0 and 
                    to_date('{ref_date}', 'YYYY-MM-DD') > rpb_contract_fix_cancel_start_date 
                    and to_date('{ref_date}', 'YYYY-MM-DD') <= rpb_contract_fix_cancel_cancel_date
                    group by 1,3;

                    drop table if exists samp3;
                    create table samp3 as
                    select study_id, max(rpb_contract_fix_cancel_contract_number) as fam,
                    to_date('{ref_date}', 'YYYY-MM-DD') as change_date,
                    max(demographics_unduped_dob) as demographics_unduped_dob,
                    max(demographics_unduped_gender) as demographics_unduped_gender
                    from rpb_contract_fix_cancel_dob a
                    where rpb_contract_fix_cancel_fixed = 1 and 
                    to_date('{ref_date}', 'YYYY-MM-DD') > rpb_contract_fix_cancel_start_date 
                    and to_date('{ref_date}', 'YYYY-MM-DD') <= rpb_contract_fix_cancel_cancel_date
                    and not exists (select from samp4 b where a.study_id = b.study_id) 
                    group by 1,3;

                    insert into samp4 
                    select *
                    from samp3;

                    drop table if exists samp3;
                    create table samp3 as
                    select study_id, max(rpb_contract_fix_cancel_contract_number) as fam,
                    to_date('{ref_date}', 'YYYY-MM-DD') as change_date,
                    max(demographics_unduped_dob) as demographics_unduped_dob,
                    max(demographics_unduped_gender) as demographics_unduped_gender
                    from rpb_contract_fix_cancel_dob a
                    where rpb_contract_fix_cancel_fixed = 2 and 
                    to_date('{ref_date}', 'YYYY-MM-DD') > rpb_contract_fix_cancel_start_date 
                    and to_date('{ref_date}', 'YYYY-MM-DD') <= rpb_contract_fix_cancel_cancel_date
                    and not exists (select from samp4 b where a.study_id = b.study_id) 
                    group by 1,3;

                    insert into samp4 
                    select *
                    from samp3;
                    """.format(ref_date = ref_date,fn = fn)) 
        conn.commit()



# ____________________________________________________

            #  find family type 
            
# ______________________________________________________________________________________________



        cur.execute("""
                drop table if exists temp_fam_2001;
                create table temp_fam_2001 (id_oldest text, id_second text, fam text, famtype text, change_date date);
                    """)
        conn.commit()



        cur.execute("""select * from samp4 where demographics_unduped_dob is not 
        null order by fam, demographics_unduped_dob;""")
        row = cur.fetchone()
        tid_oldest = row[0]
        tid_second = " "
        tfam = row[1]
        oldest = row[3]
        change_date = row[2]
        ia_age_oldest = change_date.year - row[3].year
        if change_date.month < row[3].month:
            ia_age_oldest = ia_age_oldest - 1
        if ia_age_oldest < 19:
            t_age = "y"
        else:
            t_age = "a"
        count_members = 1
        ttype = "sn"+t_age
        rows = cur.fetchall()
        for row in rows:
            if tfam != row[1]:
                # if ttype = "2p" and tid_second = '':
                    # import pdb; pdb.set_trace()
                cur.execute("""
                insert into temp_fam_2001 values('{tid_oldest}', '{tid_second}', '{tfam}', '{ttype}' ,'{change_date}');
                """.format(tid_oldest = tid_oldest, tid_second = tid_second , change_date = change_date, tfam = tfam, ttype = ttype)) 
                tid_oldest = row[0]
                tid_second = " "
                tfam = row[1]
                oldest = row[3]
                change_date = row[2]
                ia_age_oldest = change_date.year - row[3].year
                if change_date.month < row[3].month:
                    ia_age_oldest = ia_age_oldest - 1
                if ia_age_oldest < 19:
                    t_age = "y"
                else:
                    t_age = "a"
                count_members = 1
                ttype = "sn"+t_age
            else:
                count_members = count_members + 1
                if count_members == 2:
                    tid_second = row[0]
                    ia_age = row[2].year - row[3].year
                    if row[2].month < row[3].month:
                        ia_age = ia_age - 1
                    if ia_age > 24 or (ia_age >18 and ia_age_oldest - ia_age < 17) or (ia_age >16 and ia_age_oldest - ia_age < 7):
                        ttype = "c"
                    else:
                        ttype = "1p"
                if count_members == 3 and ttype == "c":
                    ttype = "2p"

            tfam = row[1]
                        
                    
        end = timeit.default_timer()
        print(end - start, "I got here 3", yr,mon)  
        conn.commit()






        cur.execute("""
                drop table if exists {fn};
                create table {fn} as 
                select a.study_id, famtype, a.fam,
                case when (famtype = 'sna' or famtype = 'sny' or famtype = '1p')
                        or (a.study_id != id_oldest and a.study_id != id_second) then '         '
                else 
                    case when a.study_id = id_oldest then id_second
                        else id_oldest end 
                end as spouseid
                from samp4 a, temp_fam_2001 b 
                where a.fam = b.fam;
                """.format(fn = fn))
        conn.commit()

