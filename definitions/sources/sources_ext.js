const tables = [
  "tb_ext_mrd_tpd001_asseteqatmlist",
  "tb_ext_mrd_tpd001_asseteqposqrcodelist",
  "tb_ext_mrd_tpd001_branches",
  "tb_ext_mrd_tpd001_categories",
  "tb_ext_mrd_tpd001_changeimpacts",
  "tb_ext_mrd_tpd001_changepriorities",
  "tb_ext_mrd_tpd001_changeprocessingstatuses",
  "tb_ext_mrd_tpd001_changetypes",
  "tb_ext_mrd_tpd001_departments",
  "tb_ext_mrd_tpd001_incidentprocessingstatuses",
  "tb_ext_mrd_tpd001_operatorgroups",
  "tb_ext_mrd_tpd001_operators",
  "tb_ext_mrd_tpd001_persons",
  "tb_ext_mrd_tpd001_subcategories",
  "tb_ext_trs_tpd001_changedetails",
  "tb_ext_trs_tpd001_changes",
  "tb_ext_trs_tpd001_incidentdetails",
  "tb_ext_trs_tpd001_incidents",
  "tb_ext_trs_tpd001_problems",
  "tb_ext_mrd_tpd001_locations",
  "tb_ext_mrd_tpd001_assetroomassignments",
  "tb_ext_trs_mdb003_debit_transaction_m1_98",
  "tb_ext_mrd_tpd001_asseteqatmdetaillist",
  "tb_ext_mrd_tpd001_asseteqposqrcodedetaillist",
  "tb_ext_trs_tmob001_sam_samtransactionfull",
  "tb_ext_trs_mdb003_debit_transaction_m1_238",
  "tb_ext_trs_tmb001_atm_dinheiro",
  "tb_ext_trs_tmb001_atm_debito",
  "tb_ext_trs_tmb001_atm_voucher",
  "tb_ext_mrd_tpd001_incidentimpacts",
  "tb_ext_mrd_tpd001_incidentpriorities",
  "tb_ext_mrd_tpd001_incidenttypes",
  "tb_ext_mrd_tpd001_incidenturgencies",
  "tb_ext_trs_aut001_monitoramento_canais",
  "tb_ext_trs_pwb001_auditoria",
  "tb_ext_trs_pwb001_governanca",
  "tb_ext_trs_pwb001_historico_refresh"

];

tables.forEach(table => {
  declare({
    database: "dev-autopass-bi-001",
    schema: "ext",
    name: table
  });
});