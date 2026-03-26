select
    "start" as condition_start_datetime,
    "stop" as condition_end_datetime,
    "patient" as patient_id,
    "encounter" as encounter_id,
    "system" as condition_system,
    "code" as condition_code,
    "description" as condition_description
from {{ source('raw', 'CONDITIONS') }}