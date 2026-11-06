import pandas as pd
import random
from datetime import datetime, timedelta


# 데이터를 저장할 빈 리스트
data = []


# 로봇 ID
robots = [
    "RBT_001",
    "RBT_002",
    "RBT_003",
    "RBT_004",
    "RBT_005",
    "RBT_006"
]


# 데이터 시작 시간
start_time = datetime(2026, 9, 15, 0, 0, 0)


# 1440분 = 24시간
for minute in range(1440):

    # 현재 시간
    timestamp = start_time + timedelta(minutes=minute)


    # 6대 로봇 각각에 대해 데이터 생성
    for robot_id in robots:

        # -------------------------
        # 기본 센서값
        # -------------------------

        temperature = random.uniform(35, 60)
        battery = random.uniform(20, 100)
        speed = random.uniform(0, 3)
        vibration = random.uniform(0.1, 0.5)
        motor_current = random.uniform(2, 5)

        error_code = "NORMAL"
        status = "RUNNING"


        # -------------------------
        # 로봇별 특성
        # -------------------------

        # RBT_003
        # 시간이 지날수록 온도가 상승
        if robot_id == "RBT_003":

            temperature = (
                40
                + (minute / 1439) * 30
                + random.uniform(-2, 2)
            )


        # RBT_004
        # 시간이 지날수록 배터리가 빠르게 감소
        elif robot_id == "RBT_004":

            battery = (
                100
                - (minute / 1439) * 90
                + random.uniform(-1, 1)
            )

            # 배터리가 0보다 작아지지 않도록 제한
            battery = max(0, battery)


        # RBT_005
        # 진동이 높은 로봇
        elif robot_id == "RBT_005":

            vibration = random.uniform(0.6, 1.2)


        # RBT_006
        # 에러가 자주 발생
        elif robot_id == "RBT_006":

            if random.random() < 0.10:

                error_code = random.choice([
                    "E01",
                    "E02",
                    "E04"
                ])

                status = "ERROR"


        # -------------------------
        # 데이터 한 행 추가
        # -------------------------

        data.append([
            timestamp,
            robot_id,
            round(temperature, 2),
            round(battery, 2),
            round(speed, 2),
            round(vibration, 2),
            round(motor_current, 2),
            error_code,
            status
        ])


# -------------------------
# DataFrame 생성
# -------------------------

df = pd.DataFrame(data, columns=[
    "timestamp",
    "robot_id",
    "temperature",
    "battery",
    "speed",
    "vibration",
    "motor_current",
    "error_code",
    "status"
])


# -------------------------
# CSV 파일 저장
# -------------------------

df.to_csv(
    "robot_sensor_2026-09-15.csv",
    index=False
)


# 데이터 확인
print(df.head())

print()
print("전체 데이터 개수:", len(df))