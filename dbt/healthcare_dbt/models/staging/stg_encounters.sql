select
    "id" as encounter_id,
    "start" as start_datetime,
    "stop" as end_datetime,
    "patient" as patient_id,
    "organization" as organization_id,
    "provider" as provider_id,
    "payer" as payer_id,
    "encounterclass" as encounter_class,
    "code" as encounter_code,
    "description" as encounter_description,
    "base_encounter_cost" as base_encounter_cost,
    "total_claim_cost" as total_claim_cost,
    "payer_coverage" as payer_coverage,
    "reasoncode" as reason_code,
    "reasondescription" as reason_description
from {{ source('raw', 'ENCOUNTERS') }}