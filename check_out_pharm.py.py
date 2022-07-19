# ______________________________________________________________________________________________

			# counts
# ______________________________________________________________________________________________
	

import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys
import zipfile

import timeit
start = timeit.default_timer()


    
# ______________________________________________________________________________________________

			# import ahfs - dinpin translation

# ______________________________________________________________________________________________
	
 
    
# cur.execute("""
        # drop table if exists pharmacare_ahfs_dinpin;
        # create table pharmacare_ahfs_dinpin (TC1 text, TC2 text, TC3 text, TC4 text, TC5 text, TC1_DESC text, TC2_DESC text, TC3_DESC text, TC4_DESC text, TC5_DESC text, dose text, DOSE_STREN text, DIN_PIN text, BRAND_NM text, MANUFACTURER_NM text, GENERIC_NM text, DOSAGE_FORM text, DRUG_ENTRY_ROUTE text, EFFECTIVE_DATE text)
            # """)
# conn.commit()


# with open('R:\\working\\users\\Bill\\thercode20060815.csv', 'r') as f:
    # cur.copy_expert('copy pharmacare_ahfs_dinpin from stdin with (format csv, header)',f)
    # conn.commit()

   
# ______________________________________________________________________________________________

			# pharm_numeric made in auto_doc

# ______________________________________________________________________________________________
	
# cur.execute("""
    # drop table if exists temp3;
    # create table temp3 as
    # select count(*),  extract(year from pharm_srv_date), pharm_tc_num2_ahfs 
    # from 
    # pharm_numeric left outer join 
    # pharm_tc_num2   
    # on pharm_hlth_prod_label::int = pharm_tc_num2_din::int 
    # group by 2,3
# """)
# conn.commit()

# with open('r:\\working\\users\\bill\\pharm\\cnt_tc2.csv', 'w') as f:
    # cur.copy_expert('copy temp3  to stdout with (format csv, header)',f)
    
# ______________________________________________________________________________________________

			# pharm_numeric made in auto_doc

# ______________________________________________________________________________________________
	

# cur.execute("""
    # drop table if exists pharm_numeric_tc;
    # create table pharm_numeric_tc as
    # select a.*, pharm_tc_num2_ahfs 
    # from 
    # pharm_numeric a left outer join 
    # pharm_tc_num2   
    # on pharm_hlth_prod_label::int = pharm_tc_num2_din::int 
# """)
# conn.commit()

# with open('r:\\working\\users\\bill\\pharm\\pharm_numeric_tc.csv', 'w') as f:
    # cur.copy_expert('copy pharm_numeric_tc  to stdout with (format csv, header)',f)
# conn.commit()
    

# ______________________________________________________________________________________________

			# can we attach costs?

# ______________________________________________________________________________________________
	
# cur.execute("""
    # drop table if exists temp3;
    # create table temp3 as
    # select count(*),  extract(year from pharm_srv_date), left(pharm_tc_num_ahfs,5) as tc2
    # from 
    # pharm_numeric left outer join 
    # pharm_tc_num 
    # on pharm_hlth_prod_label::int = pharm_tc_num_din::int 
    # group by 2,3
# """)
# conn.commit()

# with open('r:\\working\\users\\bill\\pharm\\cnt_tc2_oct2021.csv', 'w') as f:
    # cur.copy_expert('copy temp3  to stdout with (format csv, header)',f)
    
	
# cur.execute("""
    # drop table if exists temp_bad;
    # create table temp_bad as
    # select count(*) as num_scrip,  extract(year from pharm_srv_date), pharm_hlth_prod_label::int as din, left(pharm_tc_num2_ahfs,5) as tc2
    # from pharm_numeric left outer join 
    # pharm_tc_num2 
    # on pharm_hlth_prod_label::int = pharm_tc_num2_din::int 
    # group by 2,3,4;
    
    # drop table if exists temp_bad2;
    # create table temp_bad2 as
    # select * from temp_bad
    # where (tc2 <> '') is not true;


# """)
# conn.commit()

# SELECT sum(num_scrip) FROM public.temp_bad2
# where 
# din not in (select (din_pin)::int from 
# pharmacare_ahfs_dinpin )
# and din not in (select (din)::int from 
# pharm_drug_pseudo )    

# select count(*), tc2, extract(year from 
# de_srv_date )
# from 
# pharmanet_dsp_rpt a, 

# pharmacare_ahfs_dinpin b
# where din_pin::integer = 
# de_hlth_prod_label::integer



# select count(*), tc4,
# tc4_desc , generic_nm from 
# pharmacare_ahfs_dinpin 
# where generic_nm like '%METHADONE%'
# group by 2,3,4


# select count(*),generic_nm from 
# pharmacare_ahfs_dinpin 
# where TC4::int = 28080808	
# group by 2
# left(hp_tc5_cd,8)::int = 28080808 

# ______________________________________________________________________________________________

			# what overlap between IA and methadone?

# ______________________________________________________________________________________________
	
 
    
# cur.execute("""
    # drop table if exists temp2;
    # create table temp2 as 
    # select count(*), de_studyid, to_char(de_srv_date, 'yyyymm' ) as ym
    # from pharmanet_dsp_rpt a, pharmanet_hlth_rpt b
    # where hp_din_pin ::integer = de_hlth_prod_label::integer
    # and like '%METHADONE%'
    # group by 2,3
     # """)
# conn.commit()

# cur.execute("""
    # drop table if exists temp3;
    # create table temp3 as 
    # select count(*), ym, coalesce(bcea_cases_studyid_family_type, '7') as famtype
    # from temp2 a left outer join bcea_cases_studyid  b
    # on de_studyid  = study_id and ym = bcea_oop_year_month 
    # group by 2,3
     # """)
# conn.commit()


# with open('r:\\working\\users\\bill\\pharm\\cnt_IA_methadone2.csv', 'w') as f:
    # cur.copy_expert('copy temp3  to stdout with (format csv, header)',f)
    
    
 # ______________________________________________________________________________________________

			# try again with claims data
            # what overlap between IA and methadone?

# ______________________________________________________________________________________________
	
 
    
# cur.execute("""
    # drop table if exists temp2;
    # create table temp2 as 
    # select pc_studyid, to_char(pc_srv_date, 'yyyymm' ) as ym,pc_hlth_prod_label
    # from pharmanet_clm_rpt group by 1,2,3;
     # """)
# conn.commit()


# cur.execute("""
    # drop table if exists temp3;
    # create table temp3 as 
    # select * from temp2 where isnumeric(pc_hlth_prod_label);
     # """)
# conn.commit()
    

# cur.execute("""
    # drop table if exists temp4;
    # create table temp4 as 
    # select a.* 
    # from temp3 a, pharmanet_hlth_rpt b
    # where hp_din_pin::integer = pc_hlth_prod_label::integer
    # and left(hp_tc5_cd,8)::int = 28080808;
     # """)
# conn.commit()

# cur.execute("""
    # drop table if exists temp5;
    # create table temp5 as 
    # select count(*), ym, coalesce(bcea_cases_studyid_family_type, '7') as famtype
    # from temp4 a left outer join bcea_cases_studyid  b
    # on pc_studyid  = study_id and ym = bcea_oop_year_month 
    # group by 2,3
     # """)
# conn.commit()


# with open('r:\\working\\users\\bill\\pharm\\cnt_IA_methadone_clm.csv', 'w') as f:
    # cur.copy_expert('copy temp5  to stdout with (format csv, header)',f)


# ______________________________________________________________________________________________

			# counts imply that we miss a lot by using TC4::int = 28080808
            # see what other TC4's they use

# ______________________________________________________________________________________________
	
 
    
# cur.execute("""
    # drop table if exists temp4;
    # create table temp4 as 
    # select a.de_srv_date,de_hlth_prod_label
    # from pharmanet_dsp_rpt a, temp2 b
    # where a.de_studyid = b.de_studyid;


    # drop table if exists temp5;
    # create table temp5 as 
    # select count(*), TC4, to_char(de_srv_date, 'yyyymm' ) as ym
    # from temp4 a, pharmacare_ahfs_dinpin b
    # where din_pin::integer = de_hlth_prod_label::integer
    # group by 2,3
    # having count(*) > 100
     # """)

# conn.commit()
# with open('r:\\working\\users\\bill\\pharm\\cnt_methadone_alternative.csv', 'w') as f:
    # cur.copy_expert('copy temp5  to stdout with (format csv, header)',f)



