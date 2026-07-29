const tables = [
"tb_ext_mrd_shp001_depara_produtos",
"tb_ext_mrd_shp001_depara_estacao",
"tb_ext_mrd_shp001_depara_linha",
"tb_ext_mrd_shp001_depara_consorcio",
"tb_ext_mrd_shp001_depara_aplicacao",
"tb_ext_trs_tmob001_venda_qrcode"

];

tables.forEach(table => {
  declare({
    database: "dev-autopass-bi-001",
    schema: "ext",
    name: table
  });
});