SET QUERY_BAND = 'Appl=CDC_ETL;BTEQLd=$TD_DB_DMT.F_EML_PROMO;BTEQScript=merge_F_EML_PROMO.sql;' FOR SESSION;

-- Check If Process Is Already Open
select 1 from $TD_DB_ETL.etl_ctl_prcss_opn_v where prcss_nm = 'MRG_DMT_F_EML_PROMO';

.if ACTIVITYCOUNT > 0 then .goto GEN_KEY_LBL; 

-- Open Process If Not Yet Opened
exec $TD_DB_USER.OPEN_ETL_PRCSS('MRG_DMT_F_EML_PROMO', NULL);

.if errorcode > 0 then .quit errorlevel;


.label GEN_KEY_LBL 

--------------------------
--Create a Volatile table
--------------------------

create volatile table VOL_F_EML_PROMO as
(select
  eml_ky, 
  src_cd,
  cast(-1 as integer) as eml_promo_ky
 from $TD_DB_DWV.EML_PROMO src  
 where not exists
    (select 1
      from $TD_DB_DMV.F_EML_PROMO tgt  
      where tgt.eml_ky = src.eml_ky
        and tgt.src_cd = src.src_cd)
      and row_updt_tms >=
        (select prcss_strt_tms
          from $TD_DB_ETL.etl_ctl_prcss_opn_v
          where prcss_nm = 'MRG_DMT_F_EML_PROMO'
        )
)with data
 primary index(eml_ky,src_cd)
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
where tbl_nm = 'F_EML_PROMO'
and  clmn_nm = 'EML_PROMO_KY'; 

.if errorcode > 0 then .quit errorlevel;

------------------------------
---Generate the surrogate key
------------------------------
update tgt
from 
  VOL_F_EML_PROMO tgt,
  (select
      eml_ky, 
      src_cd,
     (id.lst_assgnd_id + cast (row_number() over (order by eml_ky,src_cd) as integer)) EML_PROMO_KY
   from VOL_F_EML_PROMO
   join $TD_DB_ETL.LST_ASSGND_ID id
      on id.clmn_nm = 'EML_PROMO_KY'
      and id.tbl_nm = 'F_EML_PROMO'
  )src
set EML_PROMO_KY = src.EML_PROMO_KY
where tgt.eml_ky = src.eml_ky
  and tgt.src_cd = src.src_cd;

.if errorcode > 0 then .quit errorlevel;


-----------------------------
--UPDATE LST_ASSGND_ID Table
-----------------------------

update $TD_DB_ETL.LST_ASSGND_ID
set   lst_assgnd_id = coalesce((select max(EML_PROMO_KY) from VOL_F_EML_PROMO), lst_assgnd_id),
      row_updt_tms = current_timestamp at 'gmt'
where tbl_nm = 'F_EML_PROMO'
and   clmn_nm = 'EML_PROMO_KY';

.if errorcode > 0 then .quit errorlevel;

----------------
-- Main Process
----------------

merge into $TD_DB_DMT.F_EML_PROMO tgt
using (
  select
    ky.eml_promo_ky as eml_promo_ky,
    s.src_cd,
    s.eml_ky,
    tm_sent.tm_ky as sent_tm_ky,
	s.sent_dt,
    tm_all_strt.tm_ky as allctn_strt_tm_ky,
	s.allctn_strt_dt,
    tm_all_end.tm_ky as allctn_end_tm_ky,
	s.allctn_end_dt,
    s.pr_12_prds_ordr_cnt as pr_12_prds_ord_cnt,
    s.fsc_ytd_ordr_cnt as fsc_ytd_ord_cnt,
    s.pr_12_prds_rvn_amt,
    s.pr_3_prds_rvn_amt,
    s.sls_rep_nm,
    tm_lst_ordr.tm_ky as last_ord_tm_ky,
	s.last_ord_dt,
    s.row_updt_tms,
    s.row_insrt_tms,
    s.row_stat_cd
  from $TD_DB_DWV.EML_PROMO s
  left join VOL_F_EML_PROMO ky
  on (
    s.eml_ky = ky.eml_ky
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
  )TM_SENT
  on(s.sent_dt=TM_SENT.cld_dt)
  where row_updt_tms >=
    (
    select prcss_strt_tms
    from $TD_DB_ETL.etl_ctl_prcss_opn_v
    where prcss_nm = 'MRG_DMT_F_EML_PROMO'
    )
) src 
  on (
    src.eml_ky = tgt.eml_ky
    and src.src_cd = tgt.src_cd	
  )
when matched then update set
    sent_tm_ky=src.sent_tm_ky,
	sent_dt=src.sent_dt,
    allctn_strt_tm_ky=src.allctn_strt_tm_ky,
	allctn_strt_dt=src.allctn_strt_dt,
    allctn_end_tm_ky=src.allctn_end_tm_ky,
	allctn_end_dt=src.allctn_end_dt,
    sls_rep_nm=src.sls_rep_nm,
    last_ord_tm_ky=src.last_ord_tm_ky,
	last_ord_dt=src.last_ord_dt,
    fsc_ytd_ord_cnt=src.fsc_ytd_ord_cnt,
	pr_12_prds_ord_cnt=src.pr_12_prds_ord_cnt,
    pr_3_prds_rvn_amt=src.pr_3_prds_rvn_amt,
    pr_12_prds_rvn_amt=src.pr_12_prds_rvn_amt,
    row_stat_cd =
      case
        when src.row_stat_cd = 'DEL'
        then src.row_stat_cd
        else 'UPDT'
      end,
    row_updt_tms = current_timestamp at 'gmt'
when not matched then
  insert (
    eml_promo_ky,
    src_cd,
    --iss_id,	
    eml_ky,
    sent_tm_ky,
	sent_dt,
    allctn_strt_tm_ky,
	allctn_strt_dt,
    allctn_end_tm_ky,
	allctn_end_dt,
    sls_rep_nm,
    last_ord_tm_ky,
	last_ord_dt,
	--dlvrd_ind,
	--bncd_ind,
	--hldout_ind,
    fsc_ytd_ord_cnt,
	pr_12_prds_ord_cnt,
    pr_3_prds_rvn_amt,
    pr_12_prds_rvn_amt,
    --pr_3_prds_marg_amt,
    --pr_12_prds_marg_amt,
    row_updt_tms,
    row_insrt_tms,
    row_stat_cd
  ) values (
    src.eml_promo_ky,
    src.src_cd,
    src.eml_ky,
    src.sent_tm_ky,
	src.sent_dt,
    src.ALLCTN_STRT_TM_KY,
	src.allctn_strt_dt,
    src.ALLCTN_END_TM_KY,
	src.allctn_end_dt,
    src.sls_rep_nm,
    src.LAST_ORD_TM_KY,
	src.last_ord_dt,
    src.FSC_YTD_ORD_CNT,
    src.PR_12_PRDS_ORD_CNT,
    src.PR_3_PRDS_RVN_AMT,
    src.PR_12_PRDS_RVN_AMT,
    current_timestamp at 'gmt',
    current_timestamp at 'gmt',
    case
       when src.row_stat_cd ='DEL' 
       then src.row_stat_cd
       else 'NEW'
    end
  );

.if errorcode > 0 then .quit errorlevel;

ET;

------------------------
-- Drop Volatile Taable
------------------------
drop table VOL_F_EML_PROMO;

.if errorcode > 0 then .quit errorlevel;


-- Close Process
exec $TD_DB_USER.CLOSE_ETL_PRCSS('MRG_DMT_F_EML_PROMO', NULL);

.if errorcode > 0 then .quit errorlevel;