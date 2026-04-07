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
  "tb_ext_mrd_tpd001_assetroomassignments"
];

tables.forEach(table => {
  declare({
    database: "prd-autopass-ed-001",
    schema: "ext",
    name: table
  });
});