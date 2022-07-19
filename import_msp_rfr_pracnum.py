import psycopg2
conn = psycopg2.connect(database = "db-19-g01", host = "gaboury.popdata.bc.ca")
cur = conn.cursor()
import sys

import gzip
import timeit
start = timeit.default_timer()

# ______________________________________________________________________________________________

			# import rfrprac from msp_c
# ______________________________________________________________________________________________
	

# for yr in range(1990, 2017) :
    # print(yr)
    # endpart = str.strip(str((yr+1)%100))
    # endpart = endpart.zfill(2)
    # fnzip = 'R:\\DATA\\2019-04-25\\msp\\msp'+str.strip(str(yr))+'-'+endpart+'.C.dat.gz'
    # outfile = 'raw_msp_rfrprac_'+str.strip(str(yr))+'_'+endpart

    # lines_per_file = 100000
    # smallfile = None
    # small_file_name = 'C:\\Users\\bwarburton-19-g01\\test.csv'
    
    # cur.execute("""
            # drop table if exists temp;
            # create table temp(fill text);
            # drop table if exists {outfile};
            # create table {outfile} (servdate text,
            # pracnum text, 
            # spec text, 
            # rfrpracnum text, 
            # studyid text)
                # """.format(outfile = outfile))
    # conn.commit() 

    

    # with gzip.open(fnzip, 'r') as f:
        # for lineno, line in enumerate(f):
            # if lineno % lines_per_file == 0:
                # if smallfile:
                    # smallfile.close()
                    # with open(small_file_name, 'r') as g:
                        # cur.copy_expert('copy temp from stdin',g)
                    # conn.commit() 
                    # cur.execute("""
                                # insert into {outfile}
                                # select substring(fill, 2,8) as servdate,
                                # substring(fill, 10,5) as pracnum, 
                                # substring(fill, 15,2) as spec, 
                                # substring(fill, 22,5) as rfrpracnum, 
                                # substring(fill, 295,10) as 	studyid
                                # from temp;
                                # """.format(outfile = outfile))
                    # conn.commit() 
                    # cur.execute("""
                                # drop table if exists temp;
                                # create table temp(fill text);
                                # """)

                    # conn.commit() 
                # smallfile = open(small_file_name,"w")
            # a = line.decode("utf-8", "ignore")
            # mytable = a.maketrans("\\"," ")
            # smallfile.write(a.translate(mytable))
        # if smallfile:
            # smallfile.close()
            # with open(small_file_name, 'r') as g:
                # cur.copy_expert('copy temp from stdin',g)
            # conn.commit() 
            # cur.execute("""
                        # insert into {outfile}
                        # select substring(fill, 2,8) as servdate,
                        # substring(fill, 10,5) as pracnum, 
                        # substring(fill, 15,2) as spec, 
                        # substring(fill, 22,5) as rfrpracnum, 
                        # substring(fill, 295,10) as studyid
                        # from temp;
                        # """.format(outfile = outfile))
            # conn.commit() 
# ______________________________________________________________________________________________

			# eliminate reversals, etc.
# ______________________________________________________________________________________________


# for yr in range(1990, 2017) :
    # print(yr)
    # endpart = str.strip(str((yr+1)%100))
    # endpart = endpart.zfill(2)
    # infile = 'raw_msp_rfrprac_'+str.strip(str(yr))+'_'+endpart
    # outfile = 'msp_rfrprac_'+str.strip(str(yr))+'_'+endpart

    # cur.execute("""
        # create table {outfile} as 
        # select servdate,pracnum,max(spec) as spec, max(rfrpracnum) as rfrpracnum, 
        # studyid from {infile}
        # group by 1,2,5;
    # """.format(outfile = outfile,infile = infile))
    # conn.commit() 

# ______________________________________________________________________________________________

			# copy to csv
# ______________________________________________________________________________________________


# for yr in range(1990, 2017) :
    # print(yr)
    # endpart = str.strip(str((yr+1)%100))
    # endpart = endpart.zfill(2)
    # infile = 'raw_msp_rfrprac_'+str.strip(str(yr))+'_'+endpart
    # outfile = 'msp_rfrprac_'+str.strip(str(yr))+'_'+endpart

    # with open('r:\\working\\users\\bill\\divorce\\{outfile}.csv'.format(outfile = outfile), 'w') as f:
        # cur.copy_expert('copy {outfile}  to stdout with (format csv, header)'.format(outfile = outfile),f)
    
# ______________________________________________________________________________________________

			# import rfrprac from msp_a
# ______________________________________________________________________________________________
	

for yr in range(1997, 2000) :
    print(yr)
    endpart = str.strip(str((yr+1)%100))
    endpart = endpart.zfill(2)
    fnzip = 'R:\\DATA\\2019-04-25\\msp\\msp'+str.strip(str(yr))+'-'+endpart+'.A.dat.gz'
    outfile = 'raw_msp_rfrprac_A_'+str.strip(str(yr))+'_'+endpart

    lines_per_file = 100000
    smallfile = None
    small_file_name = 'C:\\Users\\bwarburton-19-g01\\test.csv'
    
    cur.execute("""
            drop table if exists temp;
            create table temp(fill text);
            drop table if exists {outfile};
            create table {outfile} (servdate text,
            pracnum text, 
            spec text, 
            rfrpracnum text, 
            studyid text)
                """.format(outfile = outfile))
    conn.commit() 

    

    with gzip.open(fnzip, 'r') as f:
        for lineno, line in enumerate(f):
            if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                    with open(small_file_name, 'r') as g:
                        cur.copy_expert('copy temp from stdin',g)
                    conn.commit() 
                    cur.execute("""
                                insert into {outfile}
                                select substring(fill, 2,8) as servdate,
                                substring(fill, 10,5) as pracnum, 
                                substring(fill, 15,2) as spec, 
                                substring(fill, 22,5) as rfrpracnum, 
                                substring(fill, 110,10) as 	studyid
                                from temp;
                                """.format(outfile = outfile))
                    conn.commit() 
                    cur.execute("""
                                drop table if exists temp;
                                create table temp(fill text);
                                """)

                    conn.commit() 
                smallfile = open(small_file_name,"w")
            a = line.decode("utf-8", "ignore")
            mytable = a.maketrans("\\"," ")
            smallfile.write(a.translate(mytable))
        if smallfile:
            smallfile.close()
            with open(small_file_name, 'r') as g:
                cur.copy_expert('copy temp from stdin',g)
            conn.commit() 
            cur.execute("""
                                insert into {outfile}
                                select substring(fill, 2,8) as servdate,
                                substring(fill, 10,5) as pracnum, 
                                substring(fill, 15,2) as spec, 
                                substring(fill, 22,5) as rfrpracnum, 
                                substring(fill, 110,10) as 	studyid
                                from temp;
                        """.format(outfile = outfile))
            conn.commit() 
# ______________________________________________________________________________________________

			# eliminate reversals, etc.
# ______________________________________________________________________________________________

lst = [1985, 1986, 1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1997, 1998, 1999]
for yr in lst :
    print(yr)
    endpart = str.strip(str((yr+1)%100))
    endpart = endpart.zfill(2)
    infile = 'raw_msp_rfrprac_A_'+str.strip(str(yr))+'_'+endpart
    outfile = 'msp_rfrprac_A_'+str.strip(str(yr))+'_'+endpart

    cur.execute("""
        create table {outfile} as 
        select servdate,pracnum,max(spec) as spec, max(rfrpracnum) as rfrpracnum, 
        studyid from {infile}
        group by 1,2,5;
    """.format(outfile = outfile,infile = infile))
    conn.commit() 

# ______________________________________________________________________________________________

			# copy to csv
# ______________________________________________________________________________________________


for yr in lst :
    print(yr)
    endpart = str.strip(str((yr+1)%100))
    endpart = endpart.zfill(2)
    infile = 'raw_msp_rfrprac_A_'+str.strip(str(yr))+'_'+endpart
    outfile = 'msp_rfrprac_A_'+str.strip(str(yr))+'_'+endpart

    with open('r:\\working\\users\\bill\\divorce\\{outfile}.csv'.format(outfile = outfile), 'w') as f:
        cur.copy_expert('copy {outfile}  to stdout with (format csv, header)'.format(outfile = outfile),f)
    

    









