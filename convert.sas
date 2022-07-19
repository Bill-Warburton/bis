
libname datalib "R:\working\users\bill\msp";
libname datain "R:\working\users\jennys\production_data";

/*
%process_one_year(msp_a=N, msp_c=Y, yearlong=2017-18, year=20172018, outfile=msp_2017_18);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2016-17, year=20162017, outfile=msp_2016_17);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2015-16, year=20152016, outfile=msp_2015_16);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2014-15, year=20142015, outfile=msp_2014_15);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2013-14, year=20132014, outfile=msp_2013_14);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2012-13, year=20122013, outfile=msp_2012_13);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2011-12, year=20112012, outfile=msp_2011_12);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2010-11, year=20102011, outfile=msp_2010_11);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2009-10, year=20092010, outfile=msp_2009_10);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2008-09, year=20082009, outfile=msp_2008_09);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2007-08, year=20072008, outfile=msp_2007_08);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2006-07, year=20062007, outfile=msp_2006_07);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2005-06, year=20052006, outfile=msp_2005_06);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2004-05, year=20042005, outfile=msp_2004_05);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2003-04, year=20032004, outfile=msp_2003_04);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2002-03, year=20022003, outfile=msp_2002_03);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2001-02, year=20012002, outfile=msp_2001_02);
%process_one_year(msp_a=N, msp_c=Y, yearlong=2000-01, year=20002001, outfile=msp_2000_01);

%process_one_year(msp_a=Y, msp_c=Y, yearlong=1999-00, year=19992000, outfile=msp_1999_00);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1998-99, year=19981999, outfile=msp_1998_99);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1997-98, year=19971998, outfile=msp_1997_98);
%process_one_year(msp_a=N, msp_c=Y, yearlong=1996-97, year=19961997, outfile=msp_1996_97);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1995-96, year=19951996, outfile=msp_1995_96);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1994-95, year=19941995, outfile=msp_1994_95);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1993-94, year=19931994, outfile=msp_1993_94);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1992-93, year=19921993, outfile=msp_1992_93);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1991-92, year=19911992, outfile=msp_1991_92);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1990-91, year=19901991, outfile=msp_1990_91);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1989-90, year=19891990, outfile=msp_1989_90);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1988-89, year=19881989, outfile=msp_1988_89);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1987-88, year=19871988, outfile=msp_1987_88);
%process_one_year(msp_a=Y, msp_c=Y, yearlong=1986-87, year=19861987, outfile=msp_1986_87);
%process_one_year(msp_a=Y, msp_c=N, yearlong=1985-86, year=19851986, outfile=msp_1985_86);
*/

/*
proc export data=datain.msp_1985_86 outfile="R:\working\users\bill\msp\msp_1985_86"
	dbms=csv;
run;
*/


/*
proc export data=datain.msp_1985_86 outfile="R:\working\users\bill\msp\msp_1985_86"
	dbms=csv;
run;

proc export data=datain.msp_1986_87 outfile="R:\working\users\bill\msp\msp_1986_87"
	dbms=csv;
run;

proc export data=datain.msp_1987_88 outfile="R:\working\users\bill\msp\msp_1987_88"
	dbms=csv;
run;

proc export data=datain.msp_1988_89 outfile="R:\working\users\bill\msp\msp_1988_89"
	dbms=csv;
run;

proc export data=datain.msp_1989_90 outfile="R:\working\users\bill\msp\msp_1989_90"
	dbms=csv;
run;

proc export data=datain.msp_1990_91 outfile="R:\working\users\bill\msp\msp_1990_91"
	dbms=csv;
run;

proc export data=datain.msp_1991_92 outfile="R:\working\users\bill\msp\msp_1991_92"
	dbms=csv;
run;

proc export data=datain.msp_1992_93 outfile="R:\working\users\bill\msp\msp_1992_93"
	dbms=csv;
run;

proc export data=datain.msp_1993_94 outfile="R:\working\users\bill\msp\msp_1993_94"
	dbms=csv;
run;

proc export data=datain.msp_1994_95 outfile="R:\working\users\bill\msp\msp_1994_95"
	dbms=csv;
run;

proc export data=datain.msp_1995_96 outfile="R:\working\users\bill\msp\msp_1995_96"
	dbms=csv;
run;

proc export data=datain.msp_1996_97 outfile="R:\working\users\bill\msp\msp_1996_97"
	dbms=csv;
run;

proc export data=datain.msp_1997_98 outfile="R:\working\users\bill\msp\msp_1997_98"
	dbms=csv;
run;

proc export data=datain.msp_1998_99 outfile="R:\working\users\bill\msp\msp_1998_99"
	dbms=csv;
run;

proc export data=datain.msp_1999_00 outfile="R:\working\users\bill\msp\msp_1999_00"
	dbms=csv;
run;
*/
proc export data=datain.msp_2000_01 outfile="R:\working\users\bill\msp\msp_2000_01"
	dbms=csv;
run;

proc export data=datain.msp_2001_02 outfile="R:\working\users\bill\msp\msp_2001_02"
	dbms=csv;
run;

proc export data=datain.msp_2002_03 outfile="R:\working\users\bill\msp\msp_2002_03"
	dbms=csv;
run;

proc export data=datain.msp_2003_04 outfile="R:\working\users\bill\msp\msp_2003_04"
	dbms=csv;
run;

proc export data=datain.msp_2004_05 outfile="R:\working\users\bill\msp\msp_2004_05"
	dbms=csv;
run;

proc export data=datain.msp_2005_06 outfile="R:\working\users\bill\msp\msp_2005_06"
	dbms=csv;
run;

proc export data=datain.msp_2006_07 outfile="R:\working\users\bill\msp\msp_2006_07"
	dbms=csv;
run;

proc export data=datain.msp_2007_08 outfile="R:\working\users\bill\msp\msp_2007_08"
	dbms=csv;
run;

proc export data=datain.msp_2008_09 outfile="R:\working\users\bill\msp\msp_2008_09"
	dbms=csv;
run;

proc export data=datain.msp_2009_10 outfile="R:\working\users\bill\msp\msp_2009_10"
	dbms=csv;
run;

proc export data=datain.msp_2010_11 outfile="R:\working\users\bill\msp\msp_2010_11"
	dbms=csv;
run;

proc export data=datain.msp_2011_12 outfile="R:\working\users\bill\msp\msp_2011_12"
	dbms=csv;
run;

proc export data=datain.msp_2012_13 outfile="R:\working\users\bill\msp\msp_2012_13"
	dbms=csv;
run;

proc export data=datain.msp_2013_14 outfile="R:\working\users\bill\msp\msp_2013_14"
	dbms=csv;
run;

proc export data=datain.msp_2014_15 outfile="R:\working\users\bill\msp\msp_2014_15"
	dbms=csv;
run;

proc export data=datain.msp_2015_16 outfile="R:\working\users\bill\msp\msp_2015_16"
	dbms=csv;
run;

proc export data=datain.msp_2016_17 outfile="R:\working\users\bill\msp\msp_2016_17"
	dbms=csv;
run;

proc export data=datain.msp_2017_18 outfile="R:\working\users\bill\msp\msp_2017_18"
	dbms=csv;
run;





