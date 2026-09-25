{{
    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('retail_store_databricks', 'employees') }}

    
{%if is_incremental() %}
    WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
{%endif %}