select
    report_id
    , document_tag_id
    , '{{ dbt_utils.pretty_time(format="%Y-%m-%d %H:%M:%S") }}' as tuva_last_run
from {{source('elation','report_document_tag')}}