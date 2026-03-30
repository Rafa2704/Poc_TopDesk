CREATE OR REPLACE EXTERNAL TABLE `dev-autopass-bi-001.tmp.ext_incidentsnapshots`
(
id STRING,
timeStamp STRING,
incidentId STRING,
duration_inSeconds STRING,
requestDate STRING,
completionDate STRING,
closureDate STRING,
responseDate STRING,
targetDate STRING,
slaTargetDate STRING,
slaReponseTargetDate STRING,
onHoldDate STRING,
majorCall STRING,
slaApplied STRING,
taskSubType STRING,
statusId STRING,
typeId STRING,
entryTypeId STRING,
priorityId STRING,
impactId STRING,
urgencyId STRING,
callerBranchId STRING,
slaServiceId STRING,
slaServiceLevelId STRING,
contractId STRING,
categoryId STRING,
subCategoryId STRING,
operatorId STRING,
operatorGroupId STRING,
supplierId STRING,
serviceWindowId STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://autopass-datalake-topdesk/landing/incidentsnapshots/2026/incidentsnapshots_*.csv.gz'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);



CREATE OR REPLACE EXTERNAL TABLE  select * from  `dev-autopass-bi-001.tmp.ext_incidentprocessingstatuses`
(
archive STRING,
id STRING,
name STRING,
`order` STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://autopass-datalake-topdesk/landing/incidentprocessingstatuses/2026/incidentprocessingstatuses_*.csv.gz'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `dev-autopass-bi-001.tmp.ext_categories`
(
status STRING,
id STRING,
name STRING,
`order` STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://autopass-datalake-topdesk/landing/categories/2026/categories_*.csv.gz'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `dev-autopass-bi-001.tmp.ext_subcategories`
(
status STRING,
id STRING,
name STRING,
`order` STRING,
mainCategoryId STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://autopass-datalake-topdesk/landing/subcategories/2026/subcategories_*.csv.gz'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);


CREATE OR REPLACE EXTERNAL TABLE `dev-autopass-bi-001.tmp.ext_asseteqatmlist`
(
Archived STRING,
ArchivedReasonId STRING,
CreationDate STRING,
Id STRING,
ModificationDate STRING,
ObjectId STRING,
Status STRING,
TemplateId STRING
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://autopass-datalake-topdesk/landing/asseteqatmlist/2026/asseteqatmlist_*.csv.gz'],
  compression = 'GZIP',
  field_delimiter = ';',
  skip_leading_rows = 1,
  quote = '"',
  allow_quoted_newlines = TRUE
);


