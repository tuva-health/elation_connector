select
    sha2(patient_id||coalesce(item.creation_time, item.last_modified)||item.cpt||bill.id, 512) as procedure_id
    , item.id as source_id
    , note.patient_id
    , note.id as encounter_id
    , null as claim_id
    , item.creation_time as procedure_date
    , 'hcpcs' as source_code_type
    , item.cpt as source_code
    , null as source_description
    , case
        when hcpcs_ii.hcpcs is not null then 'hcpcs'
    end as normalized_code_type
    , hcpcs_ii.hcpcs as normalized_code
    , hcpcs_ii.long_description as normalized_description
    , item.modifier_1
    , item.modifier_2
    , item.modifier_3
    , item.modifier_4
    , user.id as practitioner_id
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{ source('elation','bill_item') }} item
inner join {{ source('elation','bill') }} bill
  on item.bill_id = bill.id
inner join {{ source('elation','visit_note') }} note
  on bill.visit_note_id = note.id
left join {{ ref('terminology__hcpcs_level_2') }} hcpcs_ii
    on item.cpt = hcpcs_ii.hcpcs
left join {{ source('elation','user') }} user
    on note.physician_user_id = user.id
left join {{ ref('terminology__provider') }} prov
    on user.npi = prov.npi
where item.deletion_time is null
and nullif(item.cpt,'') is not null
