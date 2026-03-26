select
    p.patient_id,
    p.birth_date,
    p.gender,
    p.race,
    p.ethnicity,
    p.city,
    p.state,
    count(e.encounter_id) as total_encounters,
    min(e.start_datetime) as first_encounter_datetime,
    max(e.start_datetime) as latest_encounter_datetime,
    avg(try_to_double(e.total_claim_cost)) as avg_claim_cost
from {{ ref('stg_patients') }} p
left join {{ ref('stg_encounters') }} e
    on p.patient_id = e.patient_id
group by
    p.patient_id,
    p.birth_date,
    p.gender,
    p.race,
    p.ethnicity,
    p.city,
    p.state