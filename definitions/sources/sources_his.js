// definitions/sources/sources_his.js

const tables = [
  "tb_his_trs_pwb001_pbi_audit_dataflows",
  "tb_his_trs_pwb001_pbi_audit_lineage",
  "SamMedia",
  "sam_typetransaction",
  "sam_actiontype"
];

tables.forEach(table => {
  declare({
    database: "prd-autopass-ed-001",
    schema: "his",
    name: table
  });
});