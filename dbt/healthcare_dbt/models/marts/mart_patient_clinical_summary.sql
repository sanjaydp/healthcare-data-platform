with encounter_summary as (
    select
        patient_id,
        count(*) as total_encounters,
        min(start_datetime) as first_encounter_datetime,
        max(start_datetime) as latest_encounter_datetime
    from {{ ref('stg_encounters') }}
    group by patient_id
),
condition_summary as (
    select
        patient_id,
        count(*) as total_conditions,
        count(distinct condition_code) as distinct_conditions
    from {{ ref('stg_conditions') }}
    group by patient_id
),
observation_summary as (
    select
        patient_id,
        count(*) as total_observations,
        count(distinct observation_code) as distinct_observations
    from {{ ref('stg_observations') }}
    group by patient_id
)
select
    p.patient_id,
    p.birth_date,
    p.gender,
    p.race,
    p.ethnicity,
    p.city,
    p.state,
    coalesce(e.total_encounters, 0) as total_encounters,
    e.first_encounter_datetime,
    e.latest_encounter_datetime,
    coalesce(c.total_conditions, 0) as total_conditions,
    coalesce(c.distinct_conditions, 0) as distinct_conditions,
    coalesce(o.total_observations, 0) as total_observations,
    coalesce(o.distinct_observations, 0) as distinct_observations
from {{ ref('stg_patients') }} p
left join encounter_summary e
    on p.patient_id = e.patient_id
left join condition_summary c
    on p.patient_id = c.patient_id
left join observation_summary o
    on p.patient_id = o.patient_id