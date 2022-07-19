# ______________________________________________________________________________________________

			# import pharm
# ______________________________________________________________________________________________
	
import gzip
import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys

import timeit
start = timeit.default_timer()

        
# fnout = "pharm_1"
# fnzip = "R:\\DATA\\2019-04-25\\pharmanet\\dsp_rpt.dat.gz"
# cur.execute("""
        # drop table if exists temp_bill;
        # create table temp_bill(fill text);
        # """)
# conn.commit() 
# with gzip.open(fnzip, 'r') as f:
    # cur.copy_expert('copy temp_bill from stdin',f)
# f.close()

# cur.execute("""
        # drop table if exists {fnout};
        # create table {fnout}(like pharmanet_dsp_rpt);
        # """.format(fnout = fnout))
# conn.commit() 
# end = timeit.default_timer()
# print(yr, "have read in file", end)

# cur.execute("select fill from temp_bill ")
# rows = cur.fetchall()

# for row in rows:
    # DE_STUDYID=row[0][0:9]
    # DE_CLNT_GENDER_LABEL=row[0][10:10]
    # DE_DM_CL_MRG_CLNT_BRTH_MTH=row[0][11:16]
    # DE_DM_CL_AGE_AT_SERVICE_YEAR=row[0][19:28]
    # DE_CL_GEO_CLNT_HA_AREA_CD=row[0][29:38]
    # DE_FCTY_LABEL=row[0][41:65]
    # DE_FC_GEO_FCTY_HA_AREA_CD=row[0][66:75]
    # DE_PR_PRAC_PRSCR_PRAC_IDNT=row[0][76:80]
    # DE_PR_GEO_PRSCR_PRAC_HA_AREA_CD=row[0][81:90]
    # DE_PR_PRAC_PRSCR_PRAC_LIC_BODY_IDNT=row[0][91:100]
    # DE_PR_INFO_PRSCR_SPTY_FLG=row[0][101:101]
    # DE_PR_INFO_RCNT_CLG_SPTY_1_LABEL=row[0][102:111]
    # DE_PR_INFO_RCNT_CLG_SPTY_2_LABEL=row[0][112:121]
    # DE_HLTH_PROD_LABEL=row[0][122:131]
    # DE_SRV_DATE=row[0][132:141]
    # DE_DSPD_QTY=row[0][142:151]
    # DE_DSPD_DAYS_SPLY=row[0][152:161]
    # cur.execute("""
        # insert into {fnout} values('{DE_STUDYID}', '{DE_CLNT_GENDER_LABEL}', to_date(trim('{DE_DM_CL_MRG_CLNT_BRTH_MTH}'),'yyyymm'), cast('{DE_DM_CL_AGE_AT_SERVICE_YEAR}' as integer), '{DE_CL_GEO_CLNT_HA_AREA_CD}', '{DE_FCTY_LABEL}', '{DE_FC_GEO_FCTY_HA_AREA_CD}', '{DE_PR_PRAC_PRSCR_PRAC_IDNT}', '{DE_PR_GEO_PRSCR_PRAC_HA_AREA_CD}', '{DE_PR_PRAC_PRSCR_PRAC_LIC_BODY_IDNT}', '{DE_PR_INFO_PRSCR_SPTY_FLG}', '{DE_PR_INFO_RCNT_CLG_SPTY_1_LABEL}', '{DE_PR_INFO_RCNT_CLG_SPTY_2_LABEL}', '{DE_HLTH_PROD_LABEL}',to_date( '{DE_SRV_DATE}', 'yyyy-mm-dd'), '{DE_DSPD_QTY}', '{DE_DSPD_DAYS_SPLY}' );
    # """.format(fnout = fnout, DE_STUDYID=DE_STUDYID, DE_CLNT_GENDER_LABEL=DE_CLNT_GENDER_LABEL, DE_DM_CL_MRG_CLNT_BRTH_MTH=DE_DM_CL_MRG_CLNT_BRTH_MTH, DE_DM_CL_AGE_AT_SERVICE_YEAR=DE_DM_CL_AGE_AT_SERVICE_YEAR, DE_CL_GEO_CLNT_HA_AREA_CD=DE_CL_GEO_CLNT_HA_AREA_CD, DE_FCTY_LABEL=DE_FCTY_LABEL, DE_FC_GEO_FCTY_HA_AREA_CD=DE_FC_GEO_FCTY_HA_AREA_CD, DE_PR_PRAC_PRSCR_PRAC_IDNT=DE_PR_PRAC_PRSCR_PRAC_IDNT, DE_PR_GEO_PRSCR_PRAC_HA_AREA_CD=DE_PR_GEO_PRSCR_PRAC_HA_AREA_CD, DE_PR_PRAC_PRSCR_PRAC_LIC_BODY_IDNT=DE_PR_PRAC_PRSCR_PRAC_LIC_BODY_IDNT, DE_PR_INFO_PRSCR_SPTY_FLG=DE_PR_INFO_PRSCR_SPTY_FLG, DE_PR_INFO_RCNT_CLG_SPTY_1_LABEL=DE_PR_INFO_RCNT_CLG_SPTY_1_LABEL, DE_PR_INFO_RCNT_CLG_SPTY_2_LABEL=DE_PR_INFO_RCNT_CLG_SPTY_2_LABEL, DE_HLTH_PROD_LABEL=DE_HLTH_PROD_LABEL, DE_SRV_DATE=DE_SRV_DATE, DE_DSPD_QTY=DE_DSPD_QTY, DE_DSPD_DAYS_SPLY=DE_DSPD_DAYS_SPLY)) 
# conn.commit() 
# end = timeit.default_timer()
# print(yr, "have written first half of file", end)
#
# cur.execute("""
            # drop table if exists pharm_TC;
            # create table pharm_TC(
            # drug_code text,
            # ATC_number text,
            # ATC text,
            # ahfs_number text,
            # ahfs text,
            # atc_f text,
            # ahfs_f text);
                        # """)

# conn.commit() 
# with open('r:\\working\\users\\bill\\pharm\\ther.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_TC from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\ther_ia.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_TC from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\ther_ap.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_TC from stdin with (format csv)',f)
# f.close()
# conn.commit()

# with open('r:\\working\\users\\bill\\pharm\\ther_dr.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_TC from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# cur.execute("""
            # drop table if exists pharm_drug;
            # create table pharm_drug(
            # drug_code text,
            # cat text,
            # drug_class text,
            # din text,
            # name text,
            # descriptor text,
            # ped  text,
            # access_no text,
            # ais text,
            # update text,
            # ai_grp text,
            # class_f text,
            # brand_name_f text,
            # descriptor_f text);
                        # """)

# conn.commit() 
# with open('r:\\working\\users\\bill\\pharm\\drug.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_drug from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\drug_ia.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_drug from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\drug_dr.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_drug from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\drug_ap.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_drug from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# ______________________________________________________________________________________________

			# ingredients
# ______________________________________________________________________________________________
	


# cur.execute("""
            # drop table if exists pharm_ingred;
            # create table pharm_ingred(
            # drug_code text,
            # ingred_code text,
            # ingred text,
            # ingred_supplied text,
            # strength text,
            # strength_unit text,
            # strength_type  text,
            # dosage_value text,
            # base text,
            # dosage_unit text,
            # notes text,
            # ingred_f text,
            # strength_unit_f text,
            # strength_type_f text,
            # dosage_unit_f text);
                        # """)

# conn.commit() 
# with open('r:\\working\\users\\bill\\pharm\\ingred.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_ingred from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\ingred_ia.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_ingred from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\ingred_dr.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_ingred from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# with open('r:\\working\\users\\bill\\pharm\\ingred_ap.txt', 'r', encoding = 'utf-8') as f:
    # cur.copy_expert('copy pharm_ingred from stdin with (format csv)',f)
# f.close()
# conn.commit() 

# ______________________________________________________________________________________________

			# pseudo DIN's
# ______________________________________________________________________________________________
	


cur.execute("""
            drop table if exists pharm_drug_pseudo2;
            create table pharm_drug_pseudo2(
            rec_type text,
            name text,
            din text);
                        """)

conn.commit() 
with open('r:\\working\\users\\bill\\pharm\\drug identification numbers_misc.csv', 'r', encoding = 'utf-8') as f:
    cur.copy_expert('copy pharm_drug_pseudo2 from stdin with (format csv)',f)
f.close()
conn.commit() 


# ______________________________________________________________________________________________

			# add ahfs code to compounded 
# ______________________________________________________________________________________________
	
cur.execute("""
        drop table if exists temp;
        create table temp as 
        SELECT a.din, max(drug_code) as drug_code 
        FROM 
        pharm_drug_pseudo2 a, pharm_ingred b
        where 
             (position(trim(upper(name)) in trim(upper(ingred)))>0
            or position(trim(upper(ingred)) in trim(upper(name)))>0)
            and rec_type = 'compound'
        group by 1;
            
        drop table if exists temp2;
        create table temp2 as 
        SELECT max(ahfs_number) as rec_type, ' ' as name, a.din
        FROM 
        temp a, pharm_tc b
        where a.drug_code = b.drug_code
        group by 2,3;
        
        drop table if exists pharm_drug_pseudo3;
        create table pharm_drug_pseudo3 as 
        SELECT * FROM 
        pharm_drug_pseudo2 
        where din not in 
        (select temp2.din from temp2);
        
        insert into pharm_drug_pseudo3
        select * from temp2;
        
        
            """)

conn.commit() 

# ______________________________________________________________________________________________

			# make pharm_tc_num2
# ______________________________________________________________________________________________
	


cur.execute("""
            drop table if exists pharm_tc_num2;
            create table pharm_tc_num2 as
            select din::int as pharm_tc_num2_din, 
            max(ahfs_number)  as pharm_tc_num2_ahfs
            from pharm_tc a, pharm_drug b
            where a.drug_code = b.drug_code
            and isnumeric(din)
            group by 1;
            
            """)                    

conn.commit() 
cur.execute("""
            drop table if exists temp;
            create table temp as
            select din_pin::int as pharm_tc_num2_din,
            max(left(tc4,2) || ':'  ||  substring(tc4,3,2) || '.'  ||  substring(tc4,5,2) ||  '.'  ||  substring(tc4,7,2)) as pharm_tc_num2_ahfs
            
            from pharmacare_ahfs_dinpin
            where din_pin::int not in 
            (select pharm_tc_num2_din from pharm_tc_num2)
            group by 1;
            
            """)                    

conn.commit() 
cur.execute("""
            insert into pharm_tc_num2
            select * from temp;
            
            """)                    

conn.commit() 
cur.execute("""
            drop table if exists temp2;
            create table temp2 as
            select din::int as pharm_tc_num2_din,
            min(rec_type) as pharm_tc_num2_ahfs
            from pharm_drug_pseudo3 
            where din::int not in 
            (select pharm_tc_num2_din from pharm_tc_num2)
            group by 1;
            
            insert into pharm_tc_num2
            select * from temp2;
            
                               
            """)                    

conn.commit() 
