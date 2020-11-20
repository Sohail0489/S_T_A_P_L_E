SET QUERY_BAND = 'Appl=CDC_ETL;BTEQLd=$TD_DB_DMT.F_DRCT_ML_PROMO;BTEQScript=merge_F_DRCT_ML_PROMO.sql;' FOR SESSION;

-- Check If Process Is Already Open
select 1 from $TD_DB_ETL.etl_ctl_prcss_opn_v where prcss_nm = 'MRG_DMT_F_DRCT_ML_PROMO';

.if ACTIVITYCOUNT > 0 then .goto GEN_KEY_LBL; 

-- Open Process If Not Yet Opened
exec $TD_DB_USER.OPEN_ETL_PRCSS('MRG_DMT_F_DRCT_ML_PROMO', NULL);

.if errorcode > 0 then .quit errorlevel;


.label GEN_KEY_LBL 

------------------------------------------------
--Create a Volatile table for process timestamp
-------------------------------------------------
create volatile table VOL_F_DRCT_ML_PROMO_TMS as
(select
   prcss_strt_tms
  ,current_timestamp at 'gmt' run_prcs_strt_tms
 from $TD_DB_ETL.etl_ctl_prcss_opn_v
 where prcss_nm = 'MRG_DMT_F_DRCT_ML_PROMO'
) with data
  on commit preserve rows;
  
.if errorcode > 0 then .quit errorlevel;

--------------------------
--Create a Volatile table
--------------------------

create volatile table VOL_F_DRCT_ML_PROMO as
(select
  spls_prsn_id, 
  src_cd,
  promo_dt,
  cast(-1 as integer) as DRCT_ML_PROMO_KY
 from $TD_DB_DWV.DRCT_ML_PROMO src  
 where not exists
    (select 1
      from $TD_DB_DMV.F_DRCT_ML_PROMO tgt  
      where tgt.spls_prsn_id = src.spls_prsn_id
        and tgt.src_cd=src.src_cd
        and tgt.promo_dt=src.promo_dt)
      and row_updt_tms >=
        (select prcss_strt_tms
          from $TD_DB_ETL.etl_ctl_prcss_opn_v
          where prcss_nm = 'MRG_DMT_F_DRCT_ML_PROMO'
        )
)with data
 primary index(spls_prsn_id,src_cd,promo_dt)
 on commit preserve rows;

.if errorcode > 0 then .quit errorlevel;

----------------------------
--Begin TRANSACTION
----------------------------
BT;

----------------------------
--LOCK LST_ASSGND_ID Table
----------------------------
update $TD_DB_ETL.LST_ASSGND_ID
set lst_assgnd_id = lst_assgnd_id
where tbl_nm = 'F_DRCT_ML_PROMO'
and  clmn_nm = 'DRCT_ML_PROMO_KY'; 

.if errorcode > 0 then .quit errorlevel;

------------------------------
---Generate the surrogate key
------------------------------
update tgt
from 
  VOL_F_DRCT_ML_PROMO tgt,
  (select
      spls_prsn_id, 
      src_cd,
      promo_dt,
     (id.lst_assgnd_id + cast (row_number() over (order by spls_prsn_id,src_cd,promo_dt) as integer)) DRCT_ML_PROMO_KY
   from VOL_F_DRCT_ML_PROMO
   join $TD_DB_ETL.LST_ASSGND_ID id
      on id.clmn_nm = 'DRCT_ML_PROMO_KY'
      and id.tbl_nm = 'F_DRCT_ML_PROMO'
  )src
set DRCT_ML_PROMO_KY = src.DRCT_ML_PROMO_KY
where tgt.spls_prsn_id = src.spls_prsn_id
  and tgt.src_cd=tgt.src_cd
  and tgt.promo_dt=tgt.promo_dt;

.if errorcode > 0 then .quit errorlevel;


-----------------------------
--UPDATE LST_ASSGND_ID Table
-----------------------------

update $TD_DB_ETL.LST_ASSGND_ID
set   lst_assgnd_id = coalesce((select max(DRCT_ML_PROMO_KY) from VOL_F_DRCT_ML_PROMO), lst_assgnd_id),
      row_updt_tms = current_timestamp at 'gmt'
where tbl_nm = 'F_DRCT_ML_PROMO'
and   clmn_nm = 'DRCT_ML_PROMO_KY';

.if errorcode > 0 then .quit errorlevel;

----------------
-- Main Process
----------------

merge into $TD_DB_DMT.F_DRCT_ML_PROMO tgt
using (
  select
    ky.DRCT_ML_PROMO_KY as drct_ml_promo_ky,
    s.src_cd,
    s.spls_prsn_id,
    case
     when s.spls_site_id is null then 0
     else coalesce(d_site.spls_site_ky,-1)
    end as spls_site_ky,
	s.promo_dt,
	tm_drp_dt.tm_ky as drop_tm_ky,
	s.drop_dt,
    tm_all_strt.tm_ky as allctn_strt_tm_ky,
	s.allctn_strt_dt,
    tm_all_end.tm_ky as allctn_end_tm_ky,
	s.allctn_end_dt,
    tm_lst_ordr.tm_ky as last_ord_tm_ky,
	s.last_ord_dt,
    s.promo_typ_cd as promo_typ_cd,
    s.pdt_prmry_ctgry_nm as pdt_prmry_ctgry_nm,	
    s.fsc_ytd_ordr_cnt as fsc_ytd_ord_cnt,
    s.pr_12_prds_ordr_cnt as pr_12_prds_ord_cnt,
    s.pr_3_prds_rvn_amt,
    s.pr_12_prds_rvn_amt,
    s.sls_rep_nm,
    --tm_promo.wk_cnt as wk_cnt,
    s.row_updt_tms,
    s.row_insrt_tms,
    s.row_stat_cd
  from $TD_DB_DWV.DRCT_ML_PROMO s
  left join VOL_F_DRCT_ML_PROMO ky
  on (
    s.spls_prsn_id = ky.spls_prsn_id
    and s.promo_dt=ky.promo_dt	
	and s.src_cd = ky.src_cd
  )left join (select
  cld_dt, tm_ky
  from $TD_DB_DMV.D_CONTR_TM_V 
  )TM_ALL_STRT
  on(s.ALLCTN_STRT_DT=TM_ALL_STRT.cld_dt)
  left join (select
  cld_dt, tm_ky
  from $TD_DB_DMV.D_CONTR_TM_V 
  )TM_ALL_END
  on(s.ALLCTN_END_DT=TM_ALL_END.cld_dt)
  left join (select
  cld_dt, tm_ky
  from $TD_DB_DMV.D_CONTR_TM_V 
  )TM_LST_ORDR
  on(s.LAST_ORDR_DT=TM_LST_ORDR.cld_dt)
  left join (select
  cld_dt, tm_ky
  from $TD_DB_DMV.D_CONTR_TM_V 
  )TM_DRP_DT
  on(s.LAST_ORDR_DT=TM_DRP_DT.cld_dt)
  /*left join (select
  cld_dt, FSC_WK_IN_YR_num as wk_cnt
  from $TD_DB_DMV.D_CONTR_TM_V 
  )TM_PROMO*/
  on(s.promo_dt=TM_PROMO.cld_dt)
  left join(select 
    spls_site_ky, spls_site_id
    from $TD_DB_DMV.D_SITE
    where row_stat_cd <> 'DEL'
  )D_SITE
  on(D_SITE.spls_site_id=s.spls_site_id)
  where row_updt_tms >=
    (
    select prcss_strt_tms
    from $TD_DB_ETL.etl_ctl_prcss_opn_v
    where prcss_nm = 'MRG_DMT_F_DRCT_ML_PROMO'
    )
) src 
  on (tgt.spls_prsn_id = src.spls_prsn_id
      and tgt.src_cd=src.src_cd
      and tgt.promo_dt=src.promo_dt	
  )
when matched then update set
    allctn_strt_tm_ky=src.allctn_strt_tm_ky,
	allctn_strt_dt=src.allctn_strt_dt,
    allctn_end_tm_ky=src.allctn_end_tm_ky,
	allctn_end_dt=src.allctn_end_dt,	
    last_ord_tm_ky=src.last_ord_tm_ky,
	last_ord_dt=src.last_ord_dt,
    promo_typ_cd=src.promo_typ_cd,
    pdt_prmry_ctgry_nm=src.pdt_prmry_ctgry_nm,
    fsc_ytd_ord_cnt=src.fsc_ytd_ord_cnt,
    pr_12_prds_ord_cnt=src.pr_12_prds_ord_cnt,
    pr_3_prds_rvn_amt=src.pr_3_prds_rvn_amt,
    pr_12_prds_rvn_amt=src.pr_12_prds_rvn_amt,
    sls_rep_nm=src.sls_rep_nm,
    --wk_cnt=src.wk_cnt,
    row_stat_cd =
      case
        when src.row_stat_cd = 'DEL'
        then src.row_stat_cd
        else 'UPDT'
      end,
    row_updt_tms = current_timestamp at 'gmt'
when not matched then
  insert (
    drct_ml_promo_ky,
    src_cd,
    spls_prsn_id,
    spls_site_ky,	
    promo_dt,
    drop_tm_ky,
	drop_dt,
    allctn_strt_tm_ky,
	allctn_strt_dt,
    allctn_end_tm_ky,
	allctn_end_dt,
    last_ord_tm_ky,
	last_ord_dt,
    --hldout_ind, 	
    promo_typ_cd,
    pdt_prmry_ctgry_nm,
    fsc_ytd_ord_cnt,
    pr_12_prds_ord_cnt,
    pr_3_prds_rvn_amt,
    pr_12_prds_rvn_amt,
    --pr_3_prds_marg_amt,
    --pr_12_prds_marg_amt,	
    sls_rep_nm,
    --wk_cnt,
    row_updt_tms,
    row_insrt_tms,
    row_stat_cd
  ) values (
    src.drct_ml_promo_ky,
    src.src_cd,
    src.spls_prsn_id,
    src.spls_site_ky,	
    src.promo_dt,
    src.drop_tm_ky,
	src.drop_dt,
    src.allctn_strt_tm_ky,
	src.allctn_strt_dt,	
    src.allctn_end_tm_ky,
	src.allctn_end_dt,
    src.last_ord_tm_ky,
	src.last_ord_dt,
    --hldout_ind, 	
    src.promo_typ_cd,
    src.pdt_prmry_ctgry_nm,
    src.fsc_ytd_ord_cnt,
    src.pr_12_prds_ord_cnt,
    src.pr_3_prds_rvn_amt,
    src.pr_12_prds_rvn_amt,
    --pr_3_prds_marg_amt,
    --pr_12_prds_marg_amt,	
    src.sls_rep_nm,
    --src.wk_cnt,
    current_timestamp at 'gmt',
    current_timestamp at 'gmt',
    case
       when src.row_stat_cd ='DEL' 
       then src.row_stat_cd
       else 'NEW'
    end
  );

.if errorcode > 0 then .quit errorlevel;

------------------------------------
-- Loopback orphan records
------------------------------------
merge into $TD_DB_DMT.F_DRCT_ML_PROMO tgt
using(
  select
    lpbk.spls_prsn_id,
    lpbk.src_cd,
    lpbk.promo_dt,
    case
     when dw_src.spls_site_id is null then 0
     else coalesce(d_site.spls_site_ky,-1)
    end as spls_site_ky
  from(
    select
      spls_prsn_id,
      src_cd,
      promo_dt,
      row_updt_tms	  
	from $TD_DB_DMV.F_DRCT_ML_PROMO
    )lpbk
  join(
    select
      spls_site_id,
      spls_prsn_id,
      src_cd,
      promo_dt
    from $TD_DB_DWV.DRCT_ML_PROMO 
    )dw_src
    on (dw_src.spls_prsn_id = lpbk.spls_prsn_id
       and dw_src.src_cd = lpbk.src_cd
       and	dw_src.promo_dt = lpbk.promo_dt)
  left join(select 
        spls_site_ky, 
	    spls_site_id
      from $TD_DB_DMV.D_SITE
      where row_stat_cd <> 'DEL'
      )d_site
    on(d_site.spls_site_id=dw_src.spls_site_id)
  where lpbk.row_updt_tms < (select run_prcs_strt_tms from VOL_F_DRCT_ML_PROMO_TMS)
  )src
  on (src.spls_prsn_id = tgt.spls_prsn_id
       and src.src_cd = tgt.src_cd
       and	src.promo_dt = tgt.promo_dt
       and src.spls_site_ky <> tgt.spls_site_ky)
    when matched then update set
    spls_site_ky=src.spls_site_ky,
    row_updt_tms = current_timestamp at 'gmt';

ET;

------------------------
-- Drop Volatile Taable
------------------------
drop table VOL_F_DRCT_ML_PROMO;

.if errorcode > 0 then .quit errorlevel;


-- Close Process
exec $TD_DB_USER.CLOSE_ETL_PRCSS('MRG_DMT_F_DRCT_ML_PROMO', NULL);

.if errorcode > 0 then .quit errorlevel;