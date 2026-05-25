const tables = [
  "tb_his_mrd_clo001_card_brand",
  "tb_his_trs_clo001_receivable_analytical",
  "tb_his_mrd_clo001_product"
];

tables.forEach(table => {
  declare({
    database: "prd-autopass-bi-001",
    schema: "his",
    name: table
  });
});