{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      claim_id
    , patient_id
    , recorded_date
    , condition_type
    , normalized_code_type as code_type
    , normalized_code as code
    , data_source
from {{ ref('core__condition') }}