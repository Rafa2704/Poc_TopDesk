const tables = [
"tb_ext_mrd_shp001_depara_produtos",
"tb_ext_mrd_shp001_depara_estacao",
"tb_ext_mrd_shp001_depara_linha",
"tb_ext_mrd_shp001_depara_consorcio",
"tb_ext_mrd_shp001_depara_aplicacao"

];

tables.forEach(table => {
  declare({
    database: "dev-autopass-bi-001",
    schema: "ext",
    name: table
  });
});