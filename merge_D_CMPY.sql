SET QUERY_BAND = 'Appl=CDC_ETL;BTEQLd=$TD_DB_DMT.D_CMPY;BTEQScript=merge_D_CMPY.sql;' FOR SESSION;

-----------------------------------
-- Check If Process Is Already Open
-----------------------------------
select 1 from $TD_DB_ETL.etl_ctl_prcss_opn_v where prcss_nm = 'MRG_DMT_D_CMPY';

.if ACTIVITYCOUNT > 0 then .goto GEN_KEY_LBL;

---------------------------------
-- Open Process If Not Yet Opened
---------------------------------
exec $TD_DB_USER.OPEN_ETL_PRCSS('MRG_DMT_D_CMPY', NULL);

.if errorcode > 0 then .quit errorlevel;

.label GEN_KEY_LBL

------------------------------------------------
--Create a Volatile table for process timestamp
-------------------------------------------------
create volatile table VOL_D_CMPY_PRCS_TMS as
(select
   prcss_strt_tms
  ,current_timestamp at 'gmt' run_prcs_strt_tms
 from $TD_DB_ETL.etl_ctl_prcss_opn_v
 where prcss_nm = 'MRG_DMT_D_CMPY'
) with data
  on commit preserve rows;
  
.if errorcode > 0 then .quit errorlevel;

------------------------
-- Create Volatile Table
------------------------
create volatile table VOL_D_CMPY as
(select
    spls_cmpy_id
   ,spls_cmpy_ky
   ,row_stat_cd
 from
   (select
       src.spls_cmpy_id
      ,coalesce(tgt.spls_cmpy_ky, -1) as spls_cmpy_ky
      ,src.row_stat_cd
      ,cast('DELTA' as varchar(10)) as prcs_cd
    from $TD_DB_DWV.CMPY src
    left join $TD_DB_DMV.D_CMPY tgt
       on src.spls_cmpy_id = tgt.spls_cmpy_id
    where src.row_updt_tms >= (select prcss_strt_tms from VOL_D_CMPY_PRCS_TMS)

    union all

    select
       spls_cmpy_id
      ,spls_cmpy_ky
      ,row_stat_cd
      ,'OTHERS' as prcs_cd
    from $TD_DB_DMV.D_CMPY
   ) vol
 qualify row_number() over(partition by spls_cmpy_id order by prcs_cd asc) = 1
)with data
 primary index(spls_cmpy_id)
 on commit preserve rows;

.if errorcode > 0 then .quit errorlevel;

collect statistics on VOL_D_CMPY column(spls_cmpy_id);

----------------------------
--Begin TRANSACTION
----------------------------
BT;

----------------------------
--LOCK LST_ASSGND_ID Table
----------------------------
update $TD_DB_ETL.LST_ASSGND_ID
set lst_assgnd_id = lst_assgnd_id
where tbl_nm = 'D_CMPY'
  and clmn_nm = 'SPLS_CMPY_KY';

.if errorcode > 0 then .quit errorlevel;

----------------------------
--Generate surrogate Keys
----------------------------
update tgt
from
  VOL_D_CMPY tgt,
  (select
    vol.spls_cmpy_id as spls_cmpy_id,
    (id.lst_assgnd_id + cast (row_number() over(order by vol.spls_cmpy_id) as integer)) spls_cmpy_ky
   from VOL_D_CMPY vol
   join $TD_DB_ETL.LST_ASSGND_ID id
      on (id.clmn_nm = 'SPLS_CMPY_KY'
      and id.tbl_nm = 'D_CMPY')
   where vol.spls_cmpy_ky = -1
  )src
set spls_cmpy_ky = src.spls_cmpy_ky
where tgt.spls_cmpy_id = src.spls_cmpy_id;

.if errorcode > 0 then .quit errorlevel;

----------------------------
--UPDATE LST_ASSGND_ID Table
----------------------------
update $TD_DB_ETL.LST_ASSGND_ID
set   lst_assgnd_id = (select max(spls_cmpy_ky) from VOL_D_CMPY),
      row_updt_tms = current_timestamp at 'gmt'
where tbl_nm = 'D_CMPY'
and   clmn_nm = 'SPLS_CMPY_KY';

.if errorcode > 0 then .quit errorlevel;

------------------------------------
--Merge the delta records into target
------------------------------------
merge into $TD_DB_DMT.D_CMPY tgt
using
  (select
     ky_val.spls_cmpy_ky,
     case
       when dw_src.parnt_cmpy_id is null then 0
       else coalesce(par_ky_val.spls_cmpy_ky,-1)
     end as parnt_cmpy_ky,
     case
       when dw_src.ultmt_parnt_cmpy_id is null then 0
       else coalesce(ult_ky_val.spls_cmpy_ky,-1)
     end as ultmt_parnt_cmpy_ky,
     dw_src.spls_cmpy_id as spls_cmpy_id,
     dw_src.cmpy_nm as cmpy_nm,
     dw_src.website_url_txt as website_url_txt,
     dw_src.sic_cd as sic_cd,
     dw_src.naics_cd as naics_cd,
     dw_src.naics_dscr as naics_dscr,
     dw_src.emp_tot_cnt as emp_tot_cnt,
     dw_src.hrc_lvl_nmb as hrc_lvl_nmb,
     dw_src.hrc_src_typ_cd as hrc_src_typ_cd,
     dw_src.hrc_to_revw_ind as hrc_to_revw_ind,
     dw_src.mltpl_parnt_ind as mltpl_parnt_ind,
     dw_src.row_stat_cd as row_stat_cd
   from $TD_DB_DWV.CMPY dw_src
   join VOL_D_CMPY ky_val
     on ky_val.spls_cmpy_id = dw_src.spls_cmpy_id
   left join VOL_D_CMPY par_ky_val
     on par_ky_val.row_stat_cd <> 'DEL'
     and dw_src.parnt_cmpy_id = par_ky_val.spls_cmpy_id
   left join VOL_D_CMPY ult_ky_val
     on ult_ky_val.row_stat_cd <> 'DEL'
     and dw_src.ultmt_parnt_cmpy_id = ult_ky_val.spls_cmpy_id
   where dw_src.row_updt_tms >= (select prcss_strt_tms from VOL_D_CMPY_PRCS_TMS)
  )src
  on src.spls_cmpy_id = tgt.spls_cmpy_id
  when matched then update set
    parnt_cmpy_ky = src.parnt_cmpy_ky,
    ultmt_parnt_cmpy_ky = src.ultmt_parnt_cmpy_ky,
    cmpy_nm = src.cmpy_nm,
    website_url_txt  = src.website_url_txt,
    sic_cd = src.sic_cd,
    naics_cd = src.naics_cd,
    naics_dscr = src.naics_dscr,
    emp_tot_cnt = src.emp_tot_cnt,
    hrc_lvl_nmb = src.hrc_lvl_nmb,
    hrc_src_typ_cd = src.hrc_src_typ_cd,
    hrc_to_revw_ind = src.hrc_to_revw_ind,
    mltpl_parnt_ind = src.mltpl_parnt_ind,
    row_stat_cd =
        case
          when src.row_stat_cd = 'DEL'
          then src.row_stat_cd
          else 'UPDT'
        end,
    row_updt_tms = current_timestamp at 'gmt'
  when not matched then
    insert (
      spls_cmpy_ky,
      spls_cmpy_id,
      parnt_cmpy_ky,
      ultmt_parnt_cmpy_ky,
      cmpy_nm,
      website_url_txt,
      sic_cd,
      naics_cd,
      naics_dscr,
      emp_tot_cnt,
      hrc_lvl_nmb,
      hrc_src_typ_cd,
      hrc_to_revw_ind,
      mltpl_parnt_ind,
      row_insrt_tms,
      row_updt_tms,
      row_stat_cd
    ) values (
      src.spls_cmpy_ky,
      src.spls_cmpy_id,
      src.parnt_cmpy_ky,
      src.ultmt_parnt_cmpy_ky,
      src.cmpy_nm,
      src.website_url_txt,
      src.sic_cd,
      src.naics_cd,
      src.naics_dscr,
      src.emp_tot_cnt,
      src.hrc_lvl_nmb,
      src.hrc_src_typ_cd,
      src.hrc_to_revw_ind,
      src.mltpl_parnt_ind,
      current_timestamp at 'gmt',
      current_timestamp at 'gmt',
      case
         when src.row_stat_cd = 'DEL'
         then src.row_stat_cd
         else 'NEW'
      end
    )
;

.if errorcode > 0 then .quit errorlevel;

------------------------------------
-- Loopback orphan records
------------------------------------
merge into $TD_DB_DMT.D_CMPY tgt
using
  (select
     lpbk.spls_cmpy_id spls_cmpy_id,
     case
       when dw_src.parnt_cmpy_id is null then 0
       else coalesce(par_ky_val.spls_cmpy_ky,-1)
     end as parnt_cmpy_ky,
     case
       when dw_src.ultmt_parnt_cmpy_id is null then 0
       else coalesce(ult_ky_val.spls_cmpy_ky,-1)
     end as ultmt_parnt_cmpy_ky
   from $TD_DB_DMT.D_CMPY lpbk
   join $TD_DB_DWV.CMPY dw_src
     on dw_src.spls_cmpy_id = lpbk.spls_cmpy_id
   left join VOL_D_CMPY par_ky_val
     on par_ky_val.row_stat_cd <> 'DEL'
     and dw_src.parnt_cmpy_id = par_ky_val.spls_cmpy_id
   left join VOL_D_CMPY ult_ky_val
     on ult_ky_val.row_stat_cd <> 'DEL'
     and dw_src.ultmt_parnt_cmpy_id = ult_ky_val.spls_cmpy_id
   where lpbk.row_updt_tms < (select run_prcs_strt_tms from VOL_D_CMPY_PRCS_TMS)
    and (lpbk.parnt_cmpy_ky <> 0 or lpbk.ultmt_parnt_cmpy_ky <> 0)
  )src
    on src.spls_cmpy_id = tgt.spls_cmpy_id and
      (src.parnt_cmpy_ky <> tgt.parnt_cmpy_ky or
       src.ultmt_parnt_cmpy_ky <> tgt.ultmt_parnt_cmpy_ky)
  when matched then update set
    parnt_cmpy_ky = src.parnt_cmpy_ky,
    ultmt_parnt_cmpy_ky = src.ultmt_parnt_cmpy_ky,
    row_updt_tms = current_timestamp at 'gmt'
;

.if errorcode > 0 then .quit errorlevel;

ET;

------------------------
-- Drop Volatile Table
------------------------
drop table VOL_D_CMPY_PRCS_TMS;

.if errorcode > 0 then .quit errorlevel;

drop table VOL_D_CMPY;

.if errorcode > 0 then .quit errorlevel;

------------------------
-- Close Process
------------------------
exec $TD_DB_USER.CLOSE_ETL_PRCSS('MRG_DMT_D_CMPY', NULL);

.if errorcode > 0 then .quit errorlevel;
