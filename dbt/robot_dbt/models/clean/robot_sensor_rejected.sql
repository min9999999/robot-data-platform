{{ config(materialized='view') }}

-- CLEAN에서 제외된 행과 그 이유 (격리)
select
    *,
    case
        when EVENT_ID is null then 'EVENT_ID 없음'
        when ROBOT_ID is null then 'ROBOT_ID 없음'
        when EVENT_TIMESTAMP is null then 'EVENT_TIMESTAMP 없음'
        else '허용되지 않은 STATUS: ' || coalesce(STATUS, '(빈 값)')
    end as REJECT_REASON
from {{ ref('stg_robot_sensor') }}
where EVENT_ID is null
   or ROBOT_ID is null
   or EVENT_TIMESTAMP is null
   or STATUS is null
   or STATUS not in ('NORMAL', 'WARNING', 'DANGER')