with stage_patient as(
    select
        row_number() over (partition by coalesce(id, master_id) order by last_modified desc) as duplicate_row_count
        , id as patient_id
        , first_name
        , last_name
        , case sex
            when 'F' then 'female'
            when 'M' then 'male'
            when 'N' then 'unknown'
        end as sex
        , case race
            when 'White' then 'white'
            when 'Black or African American' then 'black or african american'
            when 'Native Hawaiian or Other Pacific Islander' then 'native hawaiian or other pacific islander'
            when 'American Indian or Alaska Native' then 'american indian or alaska native'
            when 'Asian' then 'asian'
            when 'No race specified' then 'unknown'
            when 'Declined to specify' then 'asked but unknown'
        end as race
        , dob as birth_date
        , deceased_date as death_date
        , case  
            when deceased_date is not null then 1
        end as death_flag
        , address_line1 as address
        , city
        , state.ansi_fips_state_name as state
        , zip as zip_code
        , email
        , ssn
    from {{ source('elation','patient') }} pat
    left join {{ ref('terminology__ansi_fips_state') }} state
        on pat.state = state.ansi_fips_state_abbreviation
    where (deletion_time is null
    or deleted_by_user_id is null)
)
, patient_phone as(
    select
        patient_id
        , phone as phone
        , phone_type as phone_type
        , row_number() over (partition by patient_id, phone_type order by last_modified desc) as duplicate_row_count
    from {{ source('elation','patient_phone') }}
    where is_deleted = FALSE
)
select
    pat.patient_id
    , pat.first_name
    , pat.last_name
    , pat.sex
    , pat.race
    , pat.birth_date
    , pat.death_date
    , pat.death_flag
    , pat.address
    , pat.city
    , pat.state
    , pat.zip_code
    , cast(null as {{ dbt.type_string() }} ) as county
    , cast(null as {{ dbt.type_string() }} ) as latitude
    , cast(null as {{ dbt.type_string() }} ) as longitude
    , home.phone as home_phone
    , cell.phone as mobile_phone
    , work_phone.phone as work_phone
    , other.phone as other_phone
    , pat.email
    , cast(null as {{ dbt.type_string() }} ) as ssn
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from stage_patient pat
left join patient_phone home
    on pat.patient_id = home.patient_id
    and home.phone_type = 'Home'
    and home.duplicate_row_count = 1
left join patient_phone cell
    on pat.patient_id = cell.patient_id
    and cell.phone_type = 'Cell Phone'
    and cell.duplicate_row_count = 1
left join patient_phone work_phone
    on pat.patient_id = work_phone.patient_id
    and work_phone.phone_type = 'Work'
    and work_phone.duplicate_row_count = 1
left join patient_phone other
    on pat.patient_id = other.patient_id
    and other.phone_type = 'Other'
    and other.duplicate_row_count = 1
where pat.duplicate_row_count = 1
