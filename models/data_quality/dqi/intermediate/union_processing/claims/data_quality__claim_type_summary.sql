{% if var('claims_enabled', False) == true  -%}

select
    data_source
    ,claim_type
    ,count(distinct claim_id) as claim_count
    ,sum(paid_amount) as paid_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim')}}
group by
    data_source
    ,claim_type

{% else -%}

select
    CAST(NULL AS VARCHAR) as data_source
    , CAST(NULL AS VARCHAR) as claim_type
    , CAST(NULL AS INTEGER) as claim_count
    , CAST(NULL AS DECIMAL) as paid_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run

{{ limit_zero()}}

{%- endif %}
