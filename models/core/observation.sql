{{ dbt_utils.union_relations(

    relations=[ref('elation_stage__health_maintenance'), ref('elation_stage__report')]
                , source_column_name = none

) }}