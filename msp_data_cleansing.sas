/**********************************************************************************/
/* ---                                                                        --- */
/* --- Created date: 2019-06-26                                               --- */
/* --- Created by: Jenny Sutherland                                           --- */
/* ---                                                                        --- */
/* --- The purpose of the code is to first combine layout A and C into one    --- */
/* --- file for each fiscal year and then remove MSP FFS refusals, Reversals, --- */
/* --- reversed claims and encounter reversals. The final data set contains   --- */
/* --- MSP FFS paid claims and encounter records.                             --- */
/* ---                                                                        --- */
/**********************************************************************************/

libname datalib "R:\working\users\jennys\production_data";
libname dataqa "R:\working\users\jennys\production_data\qa";

%let qa_report = R:\working\users\jennys\production_data\qa\qa_msp_data_cleansing_report_excl_servloc;

* This macro is used to import data with the msp_a data layout;

%macro import_msp_a_data(yearlong=);
%let data_path = R:\DATA\2019-04-25\msp\msp&yearlong..A.dat.gz;
filename my_data ZIP "&data_path." GZIP;

data msp_a;
	infile my_data end=finish;
	input 
	studyid $ 110-119
	subsidy $ 1-1
	@2 servdate  yymmdd8.
	pracnum  10-14
	spec  15-16
	paynum  17-21
	rfr_pracnum  22-26
	servcode  27-28
	servunits  29-32
	feeitem  33-37
	paidamt  38-44 .2
	expl_cd1 $ 45-46
	expl_cd2 $ 47-48
	expl_cd3 $ 49-50
	paiddate_o  51-58
	paidto $ 59-59
	servwhere $ 60-61
	prov $ 62-63
	servloc $ 64-64
	retro $ 65-65
	disc_amt  66-72 .2
	rback_amt  73-79 .2
	retro_amt  80-86 .2
	icd9_1 $ 87-91
	clmtype $ 92-93
;
run;

data msp_a(keep=studyid servdate pracnum feeitem servcode clmtype paynum icd9_1-icd9_5 prov servunits expdamt record_type);
	set msp_a;
	format servdate  paiddate date9.;
	length record_type $ 13 icd9_2-icd9_5 $ 5;

	icd9_2 = ''; 
	icd9_3 = '';
	icd9_4 = '';
	icd9_5 = '';

	if paiddate_o = 1 then paiddate = .;
	else if paiddate_o < 19850101 then paiddate_o = paiddate_o*100+1;

	paiddate = input(put(paiddate_o, z8.), yymmdd8.);

	if (expl_cd1 = 'RE' or expl_cd2 = 'RE' or expl_cd3 = 'RE') or 
		servcode in (65,66,67) or
		(feeitem >= 96700 and feeitem <= 96899 or feeitem >= 96002 and feeitem <= 96093 and expl_cd1  = '') then record_type = 'Enc';
	else record_type = 'FFS';

	if paidamt = . then paidamt = 0;
	if disc_amt = . then disc_amt = 0;
	if retro_amt = . then retro_amt = 0;
	if rback_amt = . then rback_amt = 0;

	expdamt = paidamt + disc_amt + retro_amt
		+ rback_amt;

	*expdamt = paidamt + retro_amt;

	if record_type = 'FFS' and expdamt = 0 and servunits = 0 then record_type = 'FFS Refusal';

	if record_type = 'Enc' and servunits < 0 then record_type = 'Enc Reversal';

	if (spec in (4,30,31,32) or (spec >= 34 and spec <=43)) then do;
		if prov = 'BC' then clmtype = 'PM';
		else if prov ne 'BC' then clmtype = 'PR';
	end;
	else if not (spec in (4,30,31,32) or (spec >= 34 and spec <=43)) then do;
		if prov = 'BC' then clmtype = 'MM';
		else if prov ne 'BC' then clmtype = 'MR';
	end;
run;

proc freq data=msp_a;
title "MSP A &yearlong: FFS, FFS Refusal, Ecnounter, Encounter Reversal :&year";
table record_type;
run;

%mend import_msp_a_data;

* This macro is used to import data with the msp_c data layout;

%macro import_msp_c_data(yearlong=);
%let data_path = R:\DATA\2019-04-25\msp\msp&yearlong..C.dat.gz;
filename my_data ZIP "&data_path." GZIP;

data msp_c;
	infile my_data end=finish;
	input 
	studyid $ 295-304
	subsidy $ 1-1
	@2 servdate yymmdd8.
	pracnum  10-14
	spec  15-16
	paynum  17-21
	rfr_pracnum  22-26
	servcode  27-28
	servunits  29-32
	feeitem  33-37
	paidamt  38-50 .2
	expl_cd1 $ 51-52
	expl_cd2 $ 53-54
	expl_cd3 $ 55-56
	@57 paiddate yymmdd8.
	paidto $ 65-65
	servwhere $ 66-67
	prov $ 68-69
	servloc $ 70-70
	retro $ 71-71
	retro_amt  72-84 .2
	icd9 $ 85-89
	clmtype $ 90-91
	rfr_flag $ 92-92
	prcntg_fee  93-97
	icd9_1 $ 98-102
	icd9_2 $ 103-107
	icd9_3 $ 108-112
	icd9_4 $ 113-117
	icd9_5 $ 118-122
	niaamt  123-135 .2
	rollamt  136-148 .2
	intamt  149-161
	pvcamt  162-174
	dscamt  175-187 .2
	prorattn  188-200 .2
	pysychprm  201-213 .2
	locumamt  214-226
	fitmrol  227-239 .2
	rvgamt  240-252 .2
	rrpamt  253-265 .2
	splamt  266-278
;
run;

data msp_c(keep=studyid servdate pracnum feeitem servcode clmtype paynum icd9_1-icd9_5 prov servunits expdamt record_type);
	set msp_c;
	format servdate  paiddate date9.;
	length record_type $ 13;

	if (expl_cd1 = 'RE' or expl_cd2 = 'RE' or expl_cd3 = 'RE') or 
		servcode in (65,66,67) or
		(feeitem >= 96700 and feeitem <= 96899 or feeitem >= 96002 and feeitem <= 96093 and expl_cd1  = '') then record_type = 'Enc';
	else record_type = 'FFS';


	if paidamt = . then paidamt = 0;
	if dscamt = . then dscamt = 0;
	if pysychprm = . then pysychprm = 0;
	if rvgamt = . then rvgamt = 0;
	if retro_amt = . then retro_amt = 0;
	if rollamt = . then rollamt = 0;
	if prorattn = . then prorattn = 0;
	if fitmrol = . then fitmrol = 0;
	if niaamt = . then niaamt = 0;
	if rrpamt = . then rrpamt = 0;

	expdamt = paidamt + dscamt + pysychprm + rvgamt + retro_amt
		+ rollamt + prorattn + fitmrol
		+ niaamt + rrpamt;

	if record_type = 'FFS' and expdamt = 0 and servunits = 0 then record_type = 'FFS Refusal';

	if record_type = 'Enc' and servunits < 0 then record_type = 'Enc Reversal';
run;

proc freq data=msp_c;
title "MSP C &yearlong: FFS, FFS Refusal, Ecnounter, Encounter Reversal :&year";
table record_type;
run;

%mend import_msp_c_data;

* This macro is used to apply MSP data cleanzing process to the MSP A and C combined dataset;

%macro process_one_year(msp_a=, msp_c=, datafile=,yearlong=, year=,  outfile=);

%if &msp_a = Y %then %do;
	%import_msp_a_data(yearlong=&yearlong);
%end;

%if &msp_c = Y %then %do;
	%import_msp_c_data(yearlong=&yearlong);
%end;

%if &msp_a = N %then %do;
	data msp_a;
		set msp_c(obs=0);
	run;
%end;

%if &msp_c = N %then %do;
	data msp_c;
		set msp_a(obs=0);
	run;
%end;

data msp;
	set msp_a msp_c;
run;

proc freq data=msp;
title "MSP Combo &yearlong: FFS, FFS Refusal, Ecnounter, Encounter Reversal :&year";
table record_type/out=stats1;
run;

proc sort data=msp out=ffs;
where record_type = 'FFS';
by studyid servdate pracnum feeitem servcode clmtype paynum icd9_1 icd9_2 icd9_3 icd9_4 icd9_5 prov;
run;

proc summary data=ffs;
var servunits expdamt;
by studyid servdate pracnum feeitem servcode clmtype paynum icd9_1 icd9_2 icd9_3 icd9_4 icd9_5 prov;
output out=ffs (drop=_type_ _freq_) sum=;
run;

data ffs;
	set ffs;
	length record_type $ 13;
	if servunits > 0 and expdamt >= 0 then record_type = 'FFS Paid';
	else if servunits = 0 then record_type = 'FFS Reversed';
	else record_type = 'FFS Unmatched';
run;

proc freq data=ffs;
title "FFS sum-grouped-by: &year";
table record_type/out=stats2;
run;

data dataqa.qa_msp_stats_&year(drop=percent);
	set stats1 stats2;
	year = &year;
run;

data ffs_paid;
	set ffs;
	where record_type = 'FFS Paid';
run;

data encounter;
	set msp;
	where record_type = 'Enc';
run;

data datalib.&outfile(keep=studyid servdate pracnum feeitem servcode clmtype paynum icd9_1 icd9_2 icd9_3 icd9_4 icd9_5 prov servunits expdamt medical);
	set ffs_paid encounter;

	if clmtype in ('MA','MB','MH','MM','MN','MS','PB','PN') and servcode < 81 then medical = 'Y';
	else medical = 'N';
run;

proc freq data=datalib.&outfile;
title "Medical: &yearlong";
table medical/out=stats3;
run;

data dataqa.qa_msp_med_stats_&year;
	set stats3;
	year = &year;
run;

%mend process_one_year;

* This macro is used to combine each MSP data cleansing qa stats into one report;
%macro qa_report(start_yr=, end_yr=);

%let year2 = %eval(&start_yr + 1);

data qa;
	retain year record_type count;
	set dataqa.qa_msp_stats_&start_yr.&year2;
run;

data med_flag;
	set dataqa.qa_msp_med_stats_&start_yr.&year2;
run;

%do year1 = &start_yr + 1 %to &end_yr;
	%let year2 = %eval(&year1 + 1);

	data qa;
		set qa dataqa.qa_msp_stats_&year1.&year2;
	run;

	data med_flag;
		set med_flag dataqa.qa_msp_med_stats_&year1.&year2;
	run;
%end;

data qa;
	set qa;
	seq = _n_;
run;

proc sort data=qa out=qa1;
where record_type in ('Enc', 'Enc Reversal','FFS','FFS Refusal');
by year;
run;

proc summary data=qa1(rename=(count=total_count));
by year;
var total_count;
output out=total_by_rec(drop=_type_ _freq_) sum=;
run;

data qa1;
	merge qa1 total_by_rec;
	by year;

	pct = round(count/total_count*100, .01);
run;

proc sort data=qa out=qa2;
*where record_type in ('FFS Paid','FFS Reversed','FFS Unmatched');
where record_type not in ('Enc', 'Enc Reversal','FFS','FFS Refusal');
by year;
run;

proc sort data=qa out=ffs(rename=(count=total_count) drop=seq record_type);
where record_type = 'FFS';
by year;
run;

data qa2;
	merge qa2 ffs;
	by year;

	pct = round(count/total_count*100, .01);
run;

data qa;
	set qa1 qa2;
run;

proc sort data=qa out=qa_msp_report(drop=seq);
by seq;
run;

proc export data=qa_msp_report outfile="&qa_report" 
	dbms=excel
	replace;
	sheet='MSP Cleansing';
run;

proc export data=med_flag outfile="&qa_report" 
	dbms=excel
	replace;
	sheet='MSP Medical';
run;

%mend qa_report;

* Program starts here;

* Call MSP data cleansing process for each MSP file;
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


* Create MSP data cleasing qa report;
%qa_report(start_yr=1985, end_yr=2016);



