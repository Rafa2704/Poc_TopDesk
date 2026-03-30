WITH BASE AS (
SELECT
  -- Campos já tabulares
  change_id,
  number,
  creationDate,
  lastModificationDate,
  briefDescription,
  status_name,
  requester_name,
  coordinator_name,
  category_name,
  subcategory_name,

  -- raw_json: campos de primeiro nível
  JSON_VALUE(raw_json, '$.templateId')        AS templateId,
  JSON_VALUE(raw_json, '$.changeType')        AS changeType,
  JSON_VALUE(raw_json, '$.requestDate')       AS requestDate,
  JSON_VALUE(raw_json, '$.processingStatus')  AS processingStatus,
  JSON_VALUE(raw_json, '$.emergencyChange')   AS emergencyChange,
  JSON_VALUE(raw_json, '$.externalNumber')    AS externalNumber,
  JSON_VALUE(raw_json, '$.archived')          AS archived,

  -- requester
  JSON_VALUE(raw_json, '$.requester.id')                    AS requester_id,
  JSON_VALUE(raw_json, '$.requester.email')                 AS requester_email,
  JSON_VALUE(raw_json, '$.requester.phoneNumber')           AS requester_phone,
  JSON_VALUE(raw_json, '$.requester.branch.name')           AS requester_branch,
  JSON_VALUE(raw_json, '$.requester.department.name')       AS requester_department,

  -- impact / benefit / priority / type
  JSON_VALUE(raw_json, '$.impact.name')       AS impact,
  JSON_VALUE(raw_json, '$.benefit.name')      AS benefit,
  JSON_VALUE(raw_json, '$.priority.name')     AS priority,
  JSON_VALUE(raw_json, '$.type.name')         AS type,

  -- status completo
  JSON_VALUE(raw_json, '$.status.id')         AS status_id,

  -- costs
  JSON_VALUE(raw_json, '$.costs.value')           AS cost_value,
  JSON_VALUE(raw_json, '$.costs.currencyPrefix')  AS cost_currency,

  -- object / asset
  JSON_VALUE(raw_json, '$.object.objectId')       AS object_id,
  JSON_VALUE(raw_json, '$.object.objectType')     AS object_type,
  JSON_VALUE(raw_json, '$.asset.id')              AS asset_id,

  -- phases > prfc
  JSON_VALUE(raw_json, '$.phases.prfc.plannedEndDate')          AS prfc_planned_end,
  JSON_VALUE(raw_json, '$.phases.prfc.endDate')                 AS prfc_end,
  JSON_VALUE(raw_json, '$.phases.prfc.authorizer.name')         AS prfc_authorizer,
  JSON_VALUE(raw_json, '$.phases.prfc.authorizer.groupName')    AS prfc_authorizer_group,

  -- phases > rfc
  JSON_VALUE(raw_json, '$.phases.rfc.plannedEndDate')           AS rfc_planned_end,
  JSON_VALUE(raw_json, '$.phases.rfc.endDate')                  AS rfc_end,
  JSON_VALUE(raw_json, '$.phases.rfc.authorizer.name')          AS rfc_authorizer,

  -- simple (fase de execução)
  JSON_VALUE(raw_json, '$.simple.plannedStartDate')             AS simple_planned_start,
  JSON_VALUE(raw_json, '$.simple.startDate')                    AS simple_start,
  JSON_VALUE(raw_json, '$.simple.plannedImplementationDate')    AS simple_planned_impl,
  JSON_VALUE(raw_json, '$.simple.implementationDate')           AS simple_impl,
  JSON_VALUE(raw_json, '$.simple.closedDate')                   AS simple_closed,
  JSON_VALUE(raw_json, '$.simple.assignee.name')                AS assignee_name,
  JSON_VALUE(raw_json, '$.simple.assignee.groupName')           AS assignee_group,

  -- optionalFields1
  JSON_VALUE(optionalFields1_json, '$.boolean1')      AS of1_boolean1,
  JSON_VALUE(optionalFields1_json, '$.date1')         AS of1_date1,
  JSON_VALUE(optionalFields1_json, '$.date2')         AS of1_date2,
  JSON_VALUE(optionalFields1_json, '$.date3')         AS of1_date3,
  JSON_VALUE(optionalFields1_json, '$.number1')       AS of1_number1,
  JSON_VALUE(optionalFields1_json, '$.number2')       AS of1_number2,
  JSON_VALUE(optionalFields1_json, '$.number3')       AS of1_number3,
  JSON_VALUE(optionalFields1_json, '$.number4')       AS of1_number4,
  JSON_VALUE(optionalFields1_json, '$.number5')       AS of1_number5,
  JSON_VALUE(optionalFields1_json, '$.text1')         AS of1_text1,
  JSON_VALUE(optionalFields1_json, '$.text2')         AS of1_text2,
  JSON_VALUE(optionalFields1_json, '$.text3')         AS of1_text3,
  JSON_VALUE(optionalFields1_json, '$.text4')         AS of1_text4,
  JSON_VALUE(optionalFields1_json, '$.text5')         AS of1_text5,
  JSON_VALUE(optionalFields1_json, '$.searchlist1')   AS of1_searchlist1,
  JSON_VALUE(optionalFields1_json, '$.searchlist2')   AS of1_searchlist2,
  JSON_VALUE(optionalFields1_json, '$.searchlist3')   AS of1_searchlist3,
  JSON_VALUE(optionalFields1_json, '$.searchlist4')   AS of1_searchlist4,
  JSON_VALUE(optionalFields1_json, '$.searchlist5')   AS of1_searchlist5,

  -- optionalFields2 (vem dentro do raw_json)
  JSON_VALUE(raw_json, '$.optionalFields2.date1')         AS of2_date1,
  JSON_VALUE(raw_json, '$.optionalFields2.date2')         AS of2_date2,
  JSON_VALUE(raw_json, '$.optionalFields2.date3')         AS of2_date3,
  JSON_VALUE(raw_json, '$.optionalFields2.date4')         AS of2_date4,
  JSON_VALUE(raw_json, '$.optionalFields2.memo1')         AS of2_memo1,
  JSON_VALUE(raw_json, '$.optionalFields2.memo2')         AS of2_memo2,
  JSON_VALUE(raw_json, '$.optionalFields2.number1')       AS of2_number1,
  JSON_VALUE(raw_json, '$.optionalFields2.number2')       AS of2_number2,
  JSON_VALUE(raw_json, '$.optionalFields2.number3')       AS of2_number3,
  JSON_VALUE(raw_json, '$.optionalFields2.text1')         AS of2_text1,
  JSON_VALUE(raw_json, '$.optionalFields2.text2')         AS of2_text2,
  JSON_VALUE(raw_json, '$.optionalFields2.text3')         AS of2_text3,
  JSON_VALUE(raw_json, '$.optionalFields2.text4')         AS of2_text4,
  JSON_VALUE(raw_json, '$.optionalFields2.text5')         AS of2_text5,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist1')   AS of2_searchlist1,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist2')   AS of2_searchlist2,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist3')   AS of2_searchlist3,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist4')   AS of2_searchlist4,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist5')   AS of2_searchlist5,

  -- optionalFields1 searchlists (são objetos {id, name})
  JSON_VALUE(optionalFields1_json, '$.searchlist1.name')   AS of1_searchlist1,
  JSON_VALUE(optionalFields1_json, '$.searchlist2.name')   AS of1_searchlist2,
  JSON_VALUE(optionalFields1_json, '$.searchlist3.name')   AS of1_searchlist3,
  JSON_VALUE(optionalFields1_json, '$.searchlist4.name')   AS of1_searchlist4,
  JSON_VALUE(optionalFields1_json, '$.searchlist5.name')   AS of1_searchlist5,

  -- optionalFields2 searchlists (idem)
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist1.name')   AS of2_searchlist1,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist2.name')   AS of2_searchlist2,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist3.name')   AS of2_searchlist3,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist4.name')   AS of2_searchlist4,
  JSON_VALUE(raw_json, '$.optionalFields2.searchlist5.name')   AS of2_searchlist5,
  

FROM `day.tb_day_trs_tpd001_operatorchanges`
)

SELECT 
    * 
FROM BASE 
WHERE number = 'M2603-804'