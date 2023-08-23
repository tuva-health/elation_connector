{{ dbt_utils.union_relations(

    relations=[ref('elation_stage__allergy'), ref('elation_stage__drug_intolerance')], source_column_name = none

) }}