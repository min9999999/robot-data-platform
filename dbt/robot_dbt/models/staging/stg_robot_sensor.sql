select
    EVENT_ID,
    ROBOT_ID,
    EVENT_TIMESTAMP,
    SCENARIO,
    TEMPERATURE,
    VIBRATION,
    MOTOR_CURRENT,
    SPEED,
    upper(STATUS) as STATUS
from {{ source('raw', 'ROBOT_SENSOR_SIMULATED') }}