{{ dbt_utils.union_relations(

    relations=[ref('elation_stage__patient_problem'), ref('elation_stage__billing_diagnosis')]
    , source_column_name = none

) }}