{{ config(
    materialized='incremental',
    unique_key='EVENT_ID',
    incremental_strategy='merge',
    alias='ROBOT_SENSOR_SIMULATED'
) }}

select *
from {{ ref('stg_robot_sensor') }}
where EVENT_ID is not null
  and ROBOT_ID is not null
  and EVENT_TIMESTAMP is not null
{% if is_incremental() %}
  and EVENT_TIMESTAMP >= (
      select dateadd('minute', -30, max(EVENT_TIMESTAMP)) from {{ this }}
  )
{% endif %}
qualify row_number() over (
    partition by EVENT_ID order by EVENT_TIMESTAMP desc
) = 1