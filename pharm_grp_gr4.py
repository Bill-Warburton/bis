from ..auto_doc import TrackedTable


class PharmGrpGr4(TrackedTable):
    name = 'pharm_grp_gr4'

    def build(self, cursor=None):
        # Put code that builds this table here.
        # don't close the cursor when you're done, that's done automatically.
        if not cursor:
            cursor = self.get_cursor()
        cursor.execute("""
            create table pharm_grp_gr4 as 
            select *,
            case when 
                pharm_tc_name_name LIKE '%NALTREXONE%' or 
                pharm_tc_name_name LIKE '%BUPRENORPHINE%' or 
                pharm_tc_name_name LIKE '%CLONIDINE%' or 
                pharm_tc_name_name LIKE '%ACAMPROSATE%' or 
                pharm_tc_name_name LIKE '%DISULFERAM%' or 
                pharm_tc_name_name LIKE '%METHADONE%' or
                pharm_tc_name_ahfs  = '28:08.08.08' then 'alc_drug'
                when 
                left(pharm_tc_name_ahfs,5) = '08:16' then 'tuberc'
                when 
                left(pharm_tc_name_ahfs,2) = '08' 
                or left(pharm_tc_name_ahfs,5) = '84:04' 
                or left(pharm_tc_name_ahfs,5) = '52:04' 
                then 'anti_infect'
                when 
                left(pharm_tc_name_ahfs,5) = '68:12'
                or left(pharm_tc_name_ahfs,2) = '32' then 'contracept'
                when 
                left(pharm_tc_name_ahfs,8) = '28:16.08' then 'anti_psy'
                when 
                left(pharm_tc_name_ahfs,8) = '28:16.04' then 'anti_dep'
                when 
                left(pharm_tc_name_ahfs,8) = '28:24.08' 
                or left(pharm_tc_name_ahfs,8) = '28:12.08' then 'benzo'
                when 
                left(pharm_tc_name_ahfs,5) = '28:24' then 'other_anxio'
                when 
                left(pharm_tc_name_ahfs,5) = '28:20' then 'stimulant'
                when 
                left(pharm_tc_name_ahfs,8) = '28:08.08' then 'opioid'
                when 
                left(pharm_tc_name_ahfs,5) = '28:08' then 'other_analges'
                else left(pharm_tc_name_ahfs,2) end as drug_group
                from pharm_tc_name
                """)

