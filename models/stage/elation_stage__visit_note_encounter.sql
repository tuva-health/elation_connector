with visit_note_diagnosis as(
    select distinct
        note.id as visit_note_id
        , replace(bill_dx.icd10_code,'.','') as icd10_code
        , row_number () over (partition by note.id order by bill_dx.seqno, bill_dx.hdb_last_sync desc) as duplicate_row_number
    from {{ source('elation','bill_item_dx')}} bill_dx
    left join {{ source('elation','bill_item')}} item
        on bill_dx.bill_item_id = item.id
    left join {{ source('elation','bill')}} bill
        on item.bill_id = bill.id
    left join {{ source('elation','visit_note')}} note
        on bill.visit_note_id = note.id
    where (bill_item_deletion_time is null
            and deletelog_id is null)
)

select 
    id as encounter_id
    , id as source_id
    , patient_id
    , 'office visit' as encounter_type
    , date(document_date) as encounter_start_date
    , date(document_date) as encounter_end_date
    , null as length_of_stay
    , null as admit_source_code
    , null as admit_source_description
    , null as admit_type_code
    , null as admit_type_description
    , null as discharge_disposition_code
    , null as discharge_disposition_description
    , null as died_flag
    , physician_user_id as attending_provider_id
    , null as facility_npi
    , 'icd-10-cm' as primary_diagnosis_code_type
    , icd10.icd_10_cm as primary_diagnosis_code
    , icd10.description as primary_diagnosis_description
    , null as ms_drg_code
    , null as ms_drg_description
    , null as apr_drg_code
    , null as apr_drg_description
    , null as paid_amount
    , null as allowed_amount
    , null as charge_amount
    , 'elation' as data_source
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','visit_note')}} note
left join visit_note_diagnosis dx
    on note.id = dx.visit_note_id
    and dx.duplicate_row_number = 1
left join {{ ref('terminology__icd_10_cm') }} icd10
on icd10.icd_10_cm = dx.icd10_code
where deletion_time is null