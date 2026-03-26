select
    "date" as observation_datetime,
    "patient" as patient_id,
    "encounter" as encounter_id,
    "category" as observation_category,
    "code" as observation_code,
    "description" as observation_description,
    "value" as observation_value,
    "units" as observation_units,
    "type" as observation_type
from {{ source('raw', 'OBSERVATIONS') }}