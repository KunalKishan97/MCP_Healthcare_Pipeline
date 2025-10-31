
  
  create view "bronze"."main"."all_data__dbt_tmp" as (
    SELECT 
    sha256(p.patientname) AS PatientName,
    p.dob,
    p.full_address,
    a.dos,
    a.appointment_status,
    d.enc_id,
    d.chart_number,
    d.icd10_1,
    d.icd10_2,
    d.icd10_3,
    d.icd10_4,
    d.icd10_5,
    d.icd10_6,
    d.icd10_7,
    d.icd10_8,
    d.icd10_9,
    d.icd10_10,
    d.icd10_11,
    d.icd10_12,
    mp.cpt,
    mp.cpt_desc,
    p.client
FROM "bronze"."main"."patient_table" p
JOIN "bronze"."main"."encounter_table" a ON p.chart_number = a.chart_number
JOIN "bronze"."main"."dx_table" d ON a.chart_number = d.chart_number
LEFT JOIN "bronze"."main"."cpt_table" mp ON a.chart_number = mp.chart_number
  );
