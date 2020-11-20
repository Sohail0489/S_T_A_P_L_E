SET QUERY_BAND = 'Appl=CDC_ETL;BTEQLd=$TD_DB_DMT.merge_SRC_TXNMY;BTEQScript=merge_SRC_TXNMY.sql;' FOR SESSION;

-- Check If Process Is Already Open
select 1 from $TD_DB_ETL.etl_ctl_prcss_opn_v where prcss_nm = 'MRG_DMT_F_SRC_TXNMY';

.if ACTIVITYCOUNT > 0 then .goto MERGE_TGT_LBL;

-- Open Process If Not Yet Opened
exec $TD_DB_USER.OPEN_ETL_PRCSS('MRG_DMT_F_SRC_TXNMY', NULL);

.if errorcode > 0 then .quit errorlevel;

--------------------------
--Create a Volatile table
--------------------------
create volatile table VOL_F_SRC_TXNMY as
 (select
   src_cd, 
   cast(-1 as integer) as src_txnmy_ky
  from $TD_DB_DWV.src_txnmy src  
  where not exists
    (select 1
       from $TD_DB_DMV.src_txnmy tgt  
    where tgt.src_cd=src.src_cd
    )
  and row_updt_tms >=
    (select prcss_strt_tms
     from $TD_DB_ETL.etl_ctl_prcss_opn_v
     where prcss_nm = 'MRG_DMT_F_SRC_TXNMY'
    )
)with data
 primary index(src_cd)
 on commit preserve rows;

.if errorcode > 0 then .quit errorlevel;

--------------------------
--Create a Volatile table
--------------------------
create volatile table VOL_DRCT_PROMO as
(SELECT
  src_cd,
  count(distinct dml_promo.spls_site_id) as promo_site_cnt,    
  count(distinct dml_promo.spls_prsn_id) as promo_usr_cnt,
  count(distinct mstr_cust_ky) as promo_mstr_cust_cnt,
FROM $TD_DB_DMV.DRCT_ML_PROMO dml_promo
LEFT JOIN (select 
  spls_site_id,
  spls_site_ky
  from $TD_DB_DMV.D_SITE
  ) d_site
  on(dml_promo.spls_site_id=d_site.spls_site_id)
LEFT JOIN (select 
  spls_site_id,
  spls_site_ky
  from $TD_DB_DMV.D_CUST_FLAT
  )d_cust
  on(d_cust.cust_lvl='SHIPTO'
	and d_cust.spls_site_ky=d_site.spls_site_ky)
  group by dml_promo.src_cd
)with data
 primary index(src_cd)
 on commit preserve rows;  

.if errorcode > 0 then .quit errorlevel;

--------------------------
--Create a Volatile table
--------------------------
create volatile table VOL_PROMO_EML as
(SELECT
  src_cd,
  count(distinct usr.mstr_cust_ky) as promo_mstr_cust_cnt,
  count(distinct eml_promo.eml_id) as promo_eml_cnt
FROM $TD_DB_DMV.EML_PROMO eml_promo
LEFT JOIN (select
  eml_id,
  eml_ky
  from $TD_DB_DMV.D_EML
  )d_eml
	 on(eml_promo.eml_id=d_eml.eml_id)
LEFT JOIN (select
  eml_ky,
  oms_usr_ky
  from $TD_DB_DMT.D_OMS_USR_EML
  )usr_eml
  on(d_eml.eml_ky=usr_eml.eml_ky)
LEFT JOIN (select
  oms_usr_ky,
  mstr_cust_ky
  from $TD_DB_DMT.D_OMS_USR
  )usr
  on(usr.oms_usr_ky = usr_eml.oms_usr_ky)
  group by eml_promo.src_cd
)with data
 primary index(src_cd)
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
where tbl_nm = 'F_SRC_TXNMY'
and  clmn_nm = 'SRC_TXNMY_KY'; 

.if errorcode > 0 then .quit errorlevel;

------------------------------
---Generate the surrogate key
------------------------------
update tgt
from 
  VOL_F_SRC_TXNMY tgt,
  (select
      src_cd,
     (id.lst_assgnd_id + cast (row_number() over (order by src_cd) as integer)) SRC_TXNMY_KY
   from VOL_F_SRC_TXNMY
   join $TD_DB_ETL.LST_ASSGND_ID id
      on id.clmn_nm = 'SRC_TXNMY_KY'
      and id.tbl_nm = 'F_SRC_TXNMY'
  )src
set SRC_TXNMY_KY = src.SRC_TXNMY_KY
where tgt.src_cd=tgt.src_cd

.if errorcode > 0 then .quit errorlevel;

-----------------------------
--UPDATE LST_ASSGND_ID Table
-----------------------------

update $TD_DB_ETL.LST_ASSGND_ID
set   lst_assgnd_id = coalesce((select max(SRC_TXNMY_KY) from VOL_F_DRCT_ML_PROMO), lst_assgnd_id),
      row_updt_tms = current_timestamp at 'gmt'
where tbl_nm = 'F_SRC_TXNMY'
and   clmn_nm = 'SRC_TXNMY_KY';

.if errorcode > 0 then .quit errorlevel;

----------------
-- Main Process
----------------
.label MERGE_TGT_LBL

merge into $TD_DB_DWT.SRC_TXNMY tgt
using(  
  SELECT
    ky.src_txnmy_ky as src_txnmy_ky,
    src_txnmy.src_cd,
    src_txnmy.pgm_cd,
    src_txnmy.pgm_nm,
	src_txnmy.cmpgn_cd,
    src_txnmy.cmpgn_nm,
    src_txnmy.cmpgn_typ_nm,
    src_txnmy.vsn_cd,
    src_txnmy.vsn_desc,
    src_txnmy.vsn_frmt_nm,
    src_txnmy.segmt_cd,
    src_txnmy.segmt_nm,
    src_txnmy.chnnl_cd,
    src_txnmy.chnnl_nm,
    src_txnmy.CST_PER_PIECE_IND,
    src_txnmy.test_cmpgn_ind,
    tm_cmpgn_strt_dt.cmpgn_strt_ky,
    tm_cmpgn_end_dt.cmpgn_end_ky,
    tm_drp_dt.drp_ky,
    src_txnmy.pdt_prmry_ctgry_nm,
    drct_promo.promo_site_cnt,
    drct_promo.promo_usr_cnt,
    coalesce(drct_promo.promo_mstr_cust_cnt,promo_eml.promo_mstr_cust_cnt) as promo_mstr_cust_cnt,
    promo_eml.promo_eml_cnt
  FROM $TD_DB_DWV.SRC_TXNMY src_txnmy
  LEFT JOIN VOL_F_SRC_TXNMY ky
  on(src_txnmy.src_cd=ky.src_cd)
  LEFT JOIN VOL_DRCT_PROMO
    on (src_txnmy.src_cd = drct_promo.src_cd)
  LEFT JOIN VOL_PROMO_EML
    on (src_txnmy.src_cd = promo_eml.src_cd)
  left join (select
    cld_dt, tm_ky
    from $TD_DB_DMV.D_CONTR_TM_V 
    )TM_CMPGN_STRT_DT
    on(src_txnmy.cmpgn_strt_dt=TM_CMPGN_STRT_DT.cld_dt)
  left join (select
    cld_dt, tm_ky
    from $TD_DB_DMV.D_CONTR_TM_V 
    )TM_CMPGN_END_DT
    on(src_txnmy.cmpgn_end_dt=TM_CMPGN_END_DT.cld_dt)
  left join (select
    cld_dt, tm_ky
    from $TD_DB_DMV.D_CONTR_TM_V 
    )TM_DRP_DT
    on(src_txnmy.LAST_ORDR_DT=TM_DRP_DT.cld_dt)
  where row_updt_tms >=
      (select prcss_strt_tms
      from $TD_DB_ETL.etl_ctl_prcss_opn_v
      where prcss_nm = 'MRG_DMT_F_SRC_TXNMY')
  )src
  on(
    src.src_cd = tgt.SRC_CD
  )
when matched then update set
  pgm_cd=src.pgm_cd,
  pgm_nm=src.pgm_nm,
  cmpgn_cd=src.cmpgn_cd,
  cmpgn_nm=src.cmpgn_nm,
  cmpgn_typ_nm=src.cmpgn_typ_nm,
  vrsn_cd=src.vrsn_cd,
  vrsn_desc=src.vrsn_desc,
  vrsn_frmt_nm=src.vrsn_frmt_nm,
  segmt_cd=src.segmt_cd,
  segmt_nm=src.segmt_nm,
  chnnl_cd=src.chnnl_cd,
  chnnl_nm=src.chnnl_nm,
  cst_per_piece_ind=src.cst_per_piece_ind,
  test_cmpgn_ind=src.test_cmpgn_ind,
  cmpgn_strt_dt=src.cmpgn_strt_dt,
  cmpgn_end_dt=src.cmpgn_end_dt,
  drp_dt=src.drp_dt,
  pdt_prmry_ctgry_nm=src.pdt_prmry_ctgry_nm,
  promo_site_cnt=src.promo_site_cnt,
  promo_usr_cnt=src.promo_usr_cnt,
  promo_mstr_cust_cnt=src.promo_mstr_cust_cnt,
  promo_eml_cnt=src.promo_eml_cnt,
  row_stat_cd =
  case
    when src.row_stat_cd = 'DEL'
    then src.row_stat_cd
    else 'UPDT'
  end,
  row_updt_tms = current_timestamp at 'gmt'  
when not matched then
  insert (
    SRC_TXNMY_KY,
    SRC_CD,
    PGM_CD,
    PGM_NM,
    CMPGN_CD,
    CMPGN_NM,
    CMPGN_TYP_NM,
    VRSN_CD,
    VRSN_DESC,
    VRSN_FRMT_NM,
    SEGMT_CD,
    SEGMT_NM,
    CHNNL_CD,
    CHNNL_NM,
    CST_PER_PIECE_IND,
    test_cmpgn_ind,
    CMPGN_STRT_DT,
    CMPGN_END_DT,
    DRP_DT,
    PDT_PRMRY_CTGRY_NM,
    promo_site_cnt,
    promo_usr_cnt,
    promo_mstr_cust_cnt,
    promo_eml_cnt,
    ROW_UPDT_TMS,
    ROW_INSRT_TMS,  
    ROW_STAT_CD 
  )values (
    src.src_txnmy_ky,
    src.SRC_CD,
    src.PGM_CD,
    src.PGM_NM,
    src.CMPGN_CD,
    src.CMPGN_NM,
    src.CMPGN_TYP_NM,
    src.VRSN_CD,
    src.VRSN_DESC,
    src.VRSN_FRMT_NM,
    src.SEGMT_CD,
    src.SEGMT_NM,
    src.CHNNL_CD,
    src.CHNNL_NM,
    src.CST_PER_PIECE_IND,
    src.test_cmpgn_ind,
    src.CMPGN_STRT_DT,
    src.CMPGN_END_DT,
    src.DRP_DT,
    src.PDT_PRMRY_CTGRY_NM,
    src.promo_site_cnt,
    src.promo_usr_cnt,
    src.promo_mstr_cust_cnt,
    src.promo_eml_cnt,
    current_timestamp at 'gmt',
    current_timestamp at 'gmt',
    case
      when src.row_stat_cd = 'DEL'
      then src.row_stat_cd
      else 'NEW'
    end 
);

.if errorcode > 0 then .quit errorlevel;

ET;
-- Close Process
exec $TD_DB_USER.CLOSE_ETL_PRCSS('MRG_DMT_F_SRC_TXNMY', NULL);

.if errorcode > 0 then .quit errorlevel;