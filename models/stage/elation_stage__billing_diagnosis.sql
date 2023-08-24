select distinct
    {{sha_hash_512('patient_id||item.creation_time||bill_dx.seqno||bill_dx.icd10_code||bill_id')}} condition_id
  , bill_id as source_id
  , note.patient_id
  , note.id as encounter_id
  , cast(null as {{ dbt.type_string() }} ) as claim_id
  , item.creation_time as recorded_date
  , cast(null as {{ dbt.type_string() }} ) as onset_date
  , cast(null as {{ dbt.type_string() }} ) as resolved_date
  , case lower(bill.billing_status) 
        when 'billed' then 'billed'
        when 'unbilled' then 'unbilled'
        when 'unresolvederror' then 'other'
  end as status
  , 'billing' as condition_type
  , 'icd-10-cm' as source_code_type
  , bill_dx.icd10_code as source_code
  , cast(null as {{ dbt.type_string() }} ) as source_description
  , case
    when norm_icd10.icd_10_cm is not null then 'icd-10-cm'
  end as normalized_code_type
  , norm_icd10.icd_10_cm as normalized_code
  , norm_icd10.description as normalized_description
  , bill_dx.seqno as rank
  , cast(null as {{ dbt.type_string() }} ) as present_on_admit_code
  , cast(null as {{ dbt.type_string() }} ) as present_on_admit_description
  , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
  , bill.last_modified as source_last_modified
from {{ source('elation','bill_item_dx')}} bill_dx
left join {{ source('elation','bill_item')}} item
    on bill_dx.bill_item_id = item.id
left join {{ source('elation','bill')}} bill
    on item.bill_id = bill.id
left join {{ source('elation','visit_note')}} note
    on note.id = bill.visit_note_id
left join {{ ref('terminology__icd_10_cm') }} norm_icd10
    on replace(bill_dx.icd10_code,'.','') = norm_icd10.icd_10_cm
where bill_dx.bill_item_deletion_time is null
and bill_dx.icd10_code is not null