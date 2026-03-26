select
    condition_code,
    condition_description,
    count(*) as total_condition_records,
    count(distinct patient_id) as unique_patients
from {{ ref('stg_conditions') }}
group by
    condition_code,
    condition_description