# queries.py: Externalized SQL query strings for contract dimension workflow

DROP_DIM_RESOLUTION_CONTRACT_STG = """
DROP TABLE IF EXISTS {wrbwrk_dim_resolution_contract_stg};
"""

CREATE_DIM_RESOLUTION_CONTRACT_STG = """
CREATE OR REPLACE TABLE {wrbwrk_dim_resolution_contract_stg} 
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true
)
AS
WITH CALC_CNTRCT_TRANSACTION AS (
    SELECT DISTINCT cntrct_bus_key
    FROM {wrb_edw_snp_vw_premium_stg_comb} stg
    JOIN {wrbwrk_dim_resolution_calc_transaction} mpp ON stg.tran_ref_id = mpp.wrb_obj_key
    UNION
    SELECT DISTINCT cntrct_bus_key
    FROM {wrb_edw_snp_vw_loss_stg_comb} stg
    JOIN {wrbwrk_dim_resolution_calc_transaction} mpp ON stg.tran_ref_id = mpp.wrb_obj_key
)
SELECT
  c.src_sys_cd,
  c.cpc_co_nbr,
  c.cntrct_bus_key,
  c.direct_assumed_cd_map_corpvalue,
  c.cntrct_type_cd_map_corpvalue,
  c.prdct_cd_map_corpvalue,
  c.own_af_cd_map_corpdesc,
  c.scr_wrb_obj_key,
  c.br_wrb_obj_key,
  c.insd_wrb_obj_key,
  c.prdcr1_wrb_obj_key,
  c.prdcr2_wrb_obj_key,
  c.prdcr3_wrb_obj_key,
  c.prdcr1_rgon_wrb_obj_key,
  c.prdcr2_rgon_wrb_obj_key,
  c.prdcr3_rgon_wrb_obj_key,
  c.ctrct_wrb_obj_key,
  c.pd_wrb_obj_key,
  c.idy_wrb_obj_key,
  scr.scr_id,
  br.br_id,
  i.insrd_id,
  prdcr1.prdcr_id AS prdcr1_prdcr_id,
  prdcr2.prdcr_id AS prdcr2_prdcr_id,
  prdcr3.prdcr_id AS prdcr3_prdcr_id,
  prgon1.rgon_id AS prdcr1_rgon_id,
  prgon2.rgon_id AS prdcr2_rgon_id,
  prgon3.rgon_id AS prdcr3_rgon_id,
  ctrct.ctrct_id,
  pd.pd_id,
  idy.idy_id,
  c.lgl_co_nbr_map_corpvalue AS lgl_ent_id,
  c.cpc_co_nbr_map_corpvalue AS cpc_id,
  c.tran_proc_ts,
  c.tran_proc_end_dt,
  COALESCE(date_format(c.prdcr1_auth_eff_dt, 'yyyyMMdd'), date_format(c.cntrct_eff_dt,'yyyyMMdd'), -2) AS uw_yr_dt_id
FROM {wrb_edw_snp_vw_contract_stg_comb_end_dt_rdm} c
JOIN CALC_CNTRCT_TRANSACTION tx_filter ON tx_filter.cntrct_bus_key = c.cntrct_bus_key
LEFT JOIN {wrbfdm_cmp_d_scr} scr ON scr.wrb_obj_key = c.scr_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_br} br ON br.wrb_obj_key = c.br.wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_insrd} i ON i.wrb_obj_key = c.insd_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_prdcr} prdcr1 ON prdcr1.wrb_obj_key = c.prdcr1_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_prdcr} prdcr2 ON prdcr2.wrb_obj_key = c.prdcr2_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_prdcr} prdcr3 ON prdcr3.wrb_obj_key = c.prdcr3_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_rgon} prgon1 ON prgon1.wrb_obj_key = c.prdcr1_rgon_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_rgon} prgon2 ON prgon2.wrb_obj_key = c.prdcr2_rgon_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_rgon} prgon3 ON prgon3.wrb_obj_key = c.prdcr3_rgon_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_ctrct} ctrct ON ctrct.wrb_obj_key = c.ctrct_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_pd} pd ON pd.wrb_obj_key = c.pd_wrb_obj_key
LEFT JOIN {wrbfdm_cmp_d_idy} idy ON idy.wrb_obj_key = c.idy_wrb_obj_key
WHERE c.latest_row = 1;
"""

CREATE_SRC_DUMMY_VIEW = """
CREATE OR REPLACE TEMP VIEW src_dummy AS SELECT CAST(1 AS INT) AS DUMMY
"""

CREATE_CP_TRASH_VIEW = """
CREATE OR REPLACE TEMP VIEW cp_Trash AS SELECT * FROM src_dummy
"""

SELECT_DIM_RESOLUTION_CONTRACT_STG = """
SELECT * FROM {wrbwrk_dim_resolution_contract_stg}
"""
