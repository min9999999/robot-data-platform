from datetime import timedelta
import os
import smtplib
import tempfile
from email.message import EmailMessage

import pendulum

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# --------------------------------------------------
# 1. 설정
# --------------------------------------------------

AWS_CONN_ID = "aws_connection_p1"
SNOWFLAKE_CONN_ID = "snowflake_conn"

S3_BUCKET = "robot-condition-platform-713015477253-ap-northeast-2-an"
S3_PREFIX = "raw/simulated/"

PROCESSED_KEYS_VARIABLE = "robot_sensor_processed_keys"

RAW_TABLE = "DEV.RAW_DATA.ROBOT_SENSOR_SIMULATED"
CLEAN_TABLE = "DEV.CLEAN.ROBOT_SENSOR_SIMULATED"   # dbt 모델 robot_sensor_clean 이 만드는 테이블

STAGE_TABLE = "TMP_ROBOT_SENSOR_SIMULATED"

# ----- dbt 설정 -----
# Dockerfile에서 별도 가상환경에 설치한 dbt, docker-compose에서 마운트한 프로젝트 폴더
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"
DBT_PROJECT_DIR = "/opt/airflow/dbt/robot_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

# ----- 알람 메일 설정 -----
SMTP_CONN_ID = "min_6302"                   # Gmail SMTP Connection (Admin > Connections)
ALERT_LOG_TABLE = "DEV.CLEAN.ROBOT_ALERT_LOG"

# Admin > Variables 에 등록
#   robot_alert_recipients : 받는 사람 (쉼표로 여러 명)  예) a@naver.com,b@gmail.com
#   robot_alert_dry_run    : "true"면 메일을 보내지 않고 로그에만 출력 (기본 "false")
ALERT_RECIPIENTS_VARIABLE = "robot_alert_recipients"
ALERT_DRY_RUN_VARIABLE = "robot_alert_dry_run"
ALERT_BOOTSTRAP_VARIABLE = "robot_alert_bootstrapped"

ALERT_COOLDOWN_MINUTES = 30   # 같은 이상이 이어질 때 다시 알리는 간격
DISPLAY_OFFSET_HOURS = 9      # 데이터는 UTC -> 메일에는 한국 시간으로 표시 (이미 KST면 0)

ALERT_LEVEL = {"WARNING": 1, "DANGER": 2}
ALERT_LABEL = {"WARNING": "경고", "DANGER": "위험"}


# --------------------------------------------------
# 2. E: S3 파일 탐색 및 처리 대상 선정
# --------------------------------------------------

def extract_s3_files():
    """
    S3에서 CSV 파일 목록을 조회하고,
    신규 파일 또는 명시적인 백필 대상 파일의 S3 key 목록을 반환한다.

    반환된 문자열 목록은 Airflow가 XCom에 저장하며,
    CSV 파일 데이터 자체는 XCom으로 전달하지 않는다.
    """

    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}

    # 특정 파일을 다시 처리하고 싶을 때 사용
    backfill_keys = conf.get("backfill_keys", [])

    if isinstance(backfill_keys, str):
        backfill_keys = [backfill_keys]

    backfill_keys = set(backfill_keys)

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # 기존에 처리 완료한 파일 목록
    processed_keys = set(
        Variable.get(
            PROCESSED_KEYS_VARIABLE,
            default_var="[]",
            deserialize_json=True,
        )
    )

    # S3 파일 목록 조회
    listed_keys = s3_hook.list_keys(
        bucket_name=S3_BUCKET,
        prefix=S3_PREFIX,
    ) or []

    # S3에서 발견한 파일과 사용자가 지정한 백필 파일을 합친다.
    candidate_keys = set(listed_keys) | backfill_keys

    # 지정된 prefix 안의 CSV 파일만 대상으로 삼는다.
    csv_keys = sorted(
        key
        for key in candidate_keys
        if key.startswith(S3_PREFIX)
        and key.lower().endswith(".csv")
    )

    # 신규 파일 또는 명시적으로 재처리할 파일만 선택
    keys_to_process = [
        key
        for key in csv_keys
        if key not in processed_keys
        or key in backfill_keys
    ]

    print(f"발견한 CSV 파일: {len(csv_keys)}개")
    print(f"이번 처리 대상: {len(keys_to_process)}개")

    for key in keys_to_process:
        print(f"[처리 대상] {key}")

    # Airflow가 이 목록을 XCom(return_value)에 저장한다.
    # CSV 내용이 아니라 S3 key 문자열만 전달한다.
    return keys_to_process


# --------------------------------------------------
# 3. L: S3 CSV 다운로드 -> Snowflake RAW
# --------------------------------------------------
# RAW -> CLEAN 정제는 dbt(robot_dbt 프로젝트)가 담당한다.

def load_raw_snowflake(ti):
    """
    E 태스크가 XCom으로 전달한 S3 key 목록을 받아
    CSV를 다운로드하고 Snowflake RAW 테이블에 적재한다.
    """

    # E 태스크가 반환한 S3 key 목록 가져오기
    keys_to_process = ti.xcom_pull(
        task_ids="extract_s3_files",
        key="return_value",
    ) or []

    print(f"RAW 적재 대상 파일: {len(keys_to_process)}개")

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    snowflake_hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    # 현재 처리 완료 파일 목록을 다시 읽는다.
    # 각 파일 처리가 성공할 때마다 목록을 갱신한다.
    processed_keys = set(
        Variable.get(
            PROCESSED_KEYS_VARIABLE,
            default_var="[]",
            deserialize_json=True,
        )
    )

    # --------------------------------------------------
    # 3-1. RAW 테이블 생성
    # --------------------------------------------------

    snowflake_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
            EVENT_ID VARCHAR,
            ROBOT_ID VARCHAR,
            EVENT_TIMESTAMP TIMESTAMP_NTZ,
            SCENARIO VARCHAR,
            TEMPERATURE FLOAT,
            VIBRATION FLOAT,
            MOTOR_CURRENT FLOAT,
            SPEED FLOAT,
            STATUS VARCHAR,
            SOURCE_FILE VARCHAR,          -- 이 행을 마지막으로 적재한 S3 파일 경로 (추적용)
            LOADED_AT TIMESTAMP_NTZ       -- 적재 시각 (UTC)
        )
    """)

    # 이미 있는 RAW 테이블에도 추적 컬럼을 추가한다. (있으면 아무 일도 안 함)
    snowflake_hook.run(f"""
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS SOURCE_FILE VARCHAR
    """)
    snowflake_hook.run(f"""
        ALTER TABLE {RAW_TABLE} ADD COLUMN IF NOT EXISTS LOADED_AT TIMESTAMP_NTZ
    """)

    # 신규 파일이 없으면 종료 (dbt 태스크는 그대로 실행된다)
    if not keys_to_process:
        print("[증분 적재] 신규 파일이 없습니다.")
        return False

    # --------------------------------------------------
    # 3-2. 파일별 처리
    # --------------------------------------------------

    for s3_key in keys_to_process:

        # E 단계에서 선정한 key 목록을 그대로 처리하며,
        # MERGE를 통해 같은 EVENT_ID의 중복 삽입을 방지한다.

        local_path = None
        conn = None

        try:
            print(f"[처리 시작] {s3_key}")

            # S3 파일 가져오기
            s3_object = s3_hook.get_key(
                key=s3_key,
                bucket_name=S3_BUCKET,
            )

            if s3_object is None:
                raise FileNotFoundError(
                    f"S3 파일을 찾을 수 없습니다: {s3_key}"
                )

            # 로컬 임시 CSV 파일 생성
            with tempfile.NamedTemporaryFile(
                prefix="robot_sensor_",
                suffix=".csv",
                delete=False,
            ) as temp_file:
                local_path = temp_file.name

            # S3 -> 로컬 다운로드
            s3_object.download_file(local_path)

            file_name = os.path.basename(local_path)
            file_uri = "file://" + os.path.abspath(local_path)

            # Snowflake 연결
            conn = snowflake_hook.get_conn()

            with conn.cursor() as cursor:

                cursor.execute("USE DATABASE DEV")
                cursor.execute("USE SCHEMA RAW_DATA")

                # 이번 CSV를 임시로 담을 테이블
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {STAGE_TABLE} (
                        EVENT_ID VARCHAR,
                        ROBOT_ID VARCHAR,
                        EVENT_TIMESTAMP VARCHAR,
                        SCENARIO VARCHAR,
                        TEMPERATURE FLOAT,
                        VIBRATION FLOAT,
                        MOTOR_CURRENT FLOAT,
                        SPEED FLOAT,
                        STATUS VARCHAR
                    )
                """)

                # CSV를 Snowflake 스테이지에 업로드
                cursor.execute(f"""
                    PUT '{file_uri}'
                    @%{STAGE_TABLE}
                    AUTO_COMPRESS=FALSE
                    OVERWRITE=TRUE
                """)

                # CSV -> 임시 테이블
                cursor.execute(f"""
                    COPY INTO {STAGE_TABLE}
                    FROM @%{STAGE_TABLE}
                    FILES = ('{file_name}')
                    FILE_FORMAT = (
                        TYPE = CSV
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    )
                    ON_ERROR = 'ABORT_STATEMENT'
                """)

                # 임시 테이블 -> RAW
                # EVENT_ID가 같으면 최신 시간의 데이터로 갱신한다.
                # 새 EVENT_ID라면 RAW에 추가한다.
                # 어느 S3 파일에서 언제 적재했는지(SOURCE_FILE, LOADED_AT)도 함께 남긴다.
                # S3 경로는 SQL 문자열에 직접 붙이지 않고 바인딩 변수(%(source_file)s)로 넘긴다.
                cursor.execute(f"""
                    MERGE INTO {RAW_TABLE} AS T
                    USING (
                        SELECT
                            EVENT_ID,
                            ROBOT_ID,
                            TO_TIMESTAMP_NTZ(EVENT_TIMESTAMP)
                                AS EVENT_TIMESTAMP,
                            SCENARIO,
                            TEMPERATURE,
                            VIBRATION,
                            MOTOR_CURRENT,
                            SPEED,
                            STATUS,
                            %(source_file)s AS SOURCE_FILE,
                            SYSDATE() AS LOADED_AT
                        FROM {STAGE_TABLE}
                        WHERE EVENT_ID IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY EVENT_ID
                            ORDER BY
                                TO_TIMESTAMP_NTZ(EVENT_TIMESTAMP) DESC
                        ) = 1
                    ) AS S
                    ON T.EVENT_ID = S.EVENT_ID

                    WHEN MATCHED AND (
                        T.EVENT_TIMESTAMP IS NULL
                        OR S.EVENT_TIMESTAMP >= T.EVENT_TIMESTAMP
                    )
                    THEN UPDATE SET
                        T.ROBOT_ID = S.ROBOT_ID,
                        T.EVENT_TIMESTAMP = S.EVENT_TIMESTAMP,
                        T.SCENARIO = S.SCENARIO,
                        T.TEMPERATURE = S.TEMPERATURE,
                        T.VIBRATION = S.VIBRATION,
                        T.MOTOR_CURRENT = S.MOTOR_CURRENT,
                        T.SPEED = S.SPEED,
                        T.STATUS = S.STATUS,
                        T.SOURCE_FILE = S.SOURCE_FILE,
                        T.LOADED_AT = S.LOADED_AT

                    WHEN NOT MATCHED THEN INSERT (
                        EVENT_ID,
                        ROBOT_ID,
                        EVENT_TIMESTAMP,
                        SCENARIO,
                        TEMPERATURE,
                        VIBRATION,
                        MOTOR_CURRENT,
                        SPEED,
                        STATUS,
                        SOURCE_FILE,
                        LOADED_AT
                    ) VALUES (
                        S.EVENT_ID,
                        S.ROBOT_ID,
                        S.EVENT_TIMESTAMP,
                        S.SCENARIO,
                        S.TEMPERATURE,
                        S.VIBRATION,
                        S.MOTOR_CURRENT,
                        S.SPEED,
                        S.STATUS,
                        S.SOURCE_FILE,
                        S.LOADED_AT
                    )
                """, {"source_file": s3_key})

            # RAW 적재가 성공한 뒤 완료 기록
            processed_keys.add(s3_key)

            Variable.set(
                PROCESSED_KEYS_VARIABLE,
                sorted(processed_keys),
                serialize_json=True,
            )

            print(f"[RAW 적재 성공] {s3_key}")

        except Exception as exc:
            # 실패한 파일은 완료 목록에 기록하지 않는다.
            print(f"[파일 처리 실패] {s3_key}: {exc}")
            raise

        finally:
            if conn is not None:
                conn.close()

            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    print("[RAW 적재 완료]")
    return True


# --------------------------------------------------
# 4. 알람 메일: 새 WARNING / DANGER 이벤트 감지 -> 이메일
# --------------------------------------------------
# 발송 기준
#   1) CLEAN의 STATUS가 WARNING / DANGER 이고, 알림 이력(ROBOT_ALERT_LOG)에 없는
#      "새 이벤트"만 대상으로 한다. (EVENT_ID 기준이라 재실행해도 두 번 처리하지 않음)
#   2) 새 이벤트는 모두 이력에 기록하고, 메일은 로봇 단위로 묶어서 한 통으로 보낸다.
#   3) 같은 로봇은 아래 중 하나일 때만 메일을 보낸다.
#        - 이전에 메일을 보낸 적이 없다 (첫 알림)
#        - 등급이 올라갔다 (WARNING -> DANGER)
#        - 마지막 메일 이후 ALERT_COOLDOWN_MINUTES 분이 지났다 (이상이 계속되는 중)
#      ROBOT_04, 05처럼 이상이 시작되면 10초마다 DANGER 이벤트가 생기는 로봇은
#      이 규칙이 없으면 5분마다 같은 메일이 가기 때문이다.
#   4) 메일 발송이 실패하면 이력을 남기지 않으므로 다음 실행에서 다시 시도한다.
#   5) dbt 테스트가 실패하면 이 태스크는 실행되지 않는다. (품질이 나쁜 데이터로 알람 X)


def pick_robots_to_alert(new_events, last_sent, cooldown_minutes=None):
    """
    new_events: [{"event_id","robot_id","ts","status","t","v","c","s"}, ...]
    last_sent : {robot_id: {"status": "DANGER", "ts": datetime}}
                (마지막으로 메일을 보낸 이벤트)
    반환      : 메일에 담을 로봇별 요약 리스트
    """
    if cooldown_minutes is None:
        cooldown_minutes = ALERT_COOLDOWN_MINUTES

    by_robot = {}
    for e in new_events:
        by_robot.setdefault(e["robot_id"], []).append(e)

    picked = []
    for robot_id, events in sorted(by_robot.items()):
        worst = max(events, key=lambda e: (ALERT_LEVEL[e["status"]], e["ts"]))
        latest_ts = max(e["ts"] for e in events)
        last = last_sent.get(robot_id)

        if last is None:
            reason = "첫 알림"
        elif ALERT_LEVEL[worst["status"]] > ALERT_LEVEL[last["status"]]:
            reason = "등급 상승"
        elif latest_ts - last["ts"] >= timedelta(minutes=cooldown_minutes):
            reason = f"{cooldown_minutes}분 이상 지속"
        else:
            continue  # 같은 이상이 이어지는 중 -> 메일 생략 (이력에는 기록)

        picked.append({
            "robot_id": robot_id,
            "status": worst["status"],
            "count": len(events),
            "first_ts": min(e["ts"] for e in events),
            "latest_ts": latest_ts,
            "worst": worst,
            "reason": reason,
        })

    return picked


def build_alert_mail(picked):
    """메일 제목과 본문을 만든다. DANGER가 먼저 오도록 정렬."""
    picked = sorted(
        picked,
        key=lambda p: (-ALERT_LEVEL[p["status"]], p["robot_id"]),
    )

    danger = [p["robot_id"] for p in picked if p["status"] == "DANGER"]
    warning = [p["robot_id"] for p in picked if p["status"] == "WARNING"]

    parts = []
    if danger:
        parts.append(f"위험 {len(danger)}대")
    if warning:
        parts.append(f"경고 {len(warning)}대")

    names = ", ".join(p["robot_id"] for p in picked)
    subject = f"[Robot Platform] {' / '.join(parts)} 감지 ({names})"

    shift = timedelta(hours=DISPLAY_OFFSET_HOURS)

    def fmt(ts):
        return (ts + shift).strftime("%m-%d %H:%M:%S")

    lines = ["새로 감지된 이상 징후입니다.", ""]

    for p in picked:
        w = p["worst"]
        lines += [
            f"■ {p['robot_id']} · {ALERT_LABEL[p['status']]} ({p['reason']})",
            f"   신규 이벤트 {p['count']}건 · "
            f"{fmt(p['first_ts'])} ~ {fmt(p['latest_ts'])}",
            f"   가장 심각한 값: 온도 {w['t']:.1f} / 진동 {w['v']:.2f} / "
            f"전류 {w['c']:.2f} / 속도 {w['s']:.2f}",
            "",
        ]

    lines.append(
        f"※ 같은 이상은 등급이 올라가거나 {ALERT_COOLDOWN_MINUTES}분이 지나기 전에는 "
        "다시 보내지 않습니다."
    )

    return subject, "\n".join(lines)


def send_alert_mail(subject, body, recipients):
    """Gmail SMTP로 발송 (테스트에 성공한 방식 그대로)"""
    conn = BaseHook.get_connection(SMTP_CONN_ID)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = conn.login
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(conn.host, conn.port or 587, timeout=30) as server:
        server.starttls()
        server.login(conn.login, conn.password)
        server.send_message(msg)


def detect_anomalies():
    """CLEAN 조회 -> 새 WARNING/DANGER 선별 -> 메일 -> 알림 이력 기록"""

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    # --------------------------------------------------
    # 4-1. 알림 이력 테이블
    # --------------------------------------------------
    hook.run(f"""
        CREATE TABLE IF NOT EXISTS {ALERT_LOG_TABLE} (
            EVENT_ID VARCHAR,
            ROBOT_ID VARCHAR,
            STATUS VARCHAR,
            EVENT_TIMESTAMP TIMESTAMP_NTZ,
            MAIL_SENT BOOLEAN,
            LOGGED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    # --------------------------------------------------
    # 4-2. 최초 1회: 지금까지 쌓인 WARNING/DANGER는 "이미 본 것"으로 기록만 한다.
    #      (배포 직후 과거 이벤트 수백 건이 한꺼번에 메일로 나가는 것을 막는다.
    #       이후에도 이상이 이어지는 로봇은 다음 실행에서 첫 알림이 간다.)
    # --------------------------------------------------
    bootstrapped = Variable.get(
        ALERT_BOOTSTRAP_VARIABLE,
        default_var="false",
    ).lower() == "true"

    if not bootstrapped:
        print("[알람 초기 구성] 기존 WARNING/DANGER 이벤트를 이력에만 기록합니다.")

        hook.run(f"""
            INSERT INTO {ALERT_LOG_TABLE}
                (EVENT_ID, ROBOT_ID, STATUS, EVENT_TIMESTAMP, MAIL_SENT)
            SELECT c.EVENT_ID, c.ROBOT_ID, c.STATUS, c.EVENT_TIMESTAMP, FALSE
            FROM {CLEAN_TABLE} AS c
            LEFT JOIN {ALERT_LOG_TABLE} AS l
                ON c.EVENT_ID = l.EVENT_ID
            WHERE c.STATUS IN ('WARNING', 'DANGER')
              AND l.EVENT_ID IS NULL
        """)

        Variable.set(ALERT_BOOTSTRAP_VARIABLE, "true")
        print("[알람 초기 구성 완료]")

    # --------------------------------------------------
    # 4-3. 이력에 없는 새 WARNING/DANGER 이벤트
    # --------------------------------------------------
    rows = hook.get_records(f"""
        SELECT c.EVENT_ID, c.ROBOT_ID, c.EVENT_TIMESTAMP, c.STATUS,
               c.TEMPERATURE, c.VIBRATION, c.MOTOR_CURRENT, c.SPEED
        FROM {CLEAN_TABLE} AS c
        LEFT JOIN {ALERT_LOG_TABLE} AS l
            ON c.EVENT_ID = l.EVENT_ID
        WHERE c.STATUS IN ('WARNING', 'DANGER')
          AND l.EVENT_ID IS NULL
        ORDER BY c.EVENT_TIMESTAMP
    """)

    new_events = [
        {
            "event_id": r[0],
            "robot_id": r[1],
            "ts": r[2],
            "status": str(r[3]).upper(),
            "t": float(r[4] or 0),
            "v": float(r[5] or 0),
            "c": float(r[6] or 0),
            "s": float(r[7] or 0),
        }
        for r in rows
    ]

    print(f"새 WARNING/DANGER 이벤트: {len(new_events)}건")

    if not new_events:
        return 0

    # --------------------------------------------------
    # 4-4. 로봇별로 마지막에 메일을 보낸 이벤트
    # --------------------------------------------------
    last_rows = hook.get_records(f"""
        SELECT ROBOT_ID, STATUS, EVENT_TIMESTAMP
        FROM {ALERT_LOG_TABLE}
        WHERE MAIL_SENT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ROBOT_ID
            ORDER BY EVENT_TIMESTAMP DESC
        ) = 1
    """)

    last_sent = {
        r[0]: {"status": str(r[1]).upper(), "ts": r[2]}
        for r in last_rows
    }

    # --------------------------------------------------
    # 4-5. 메일을 보낼 로봇 선택 -> 발송
    # --------------------------------------------------
    picked = pick_robots_to_alert(new_events, last_sent)
    alerted = sorted({p["robot_id"] for p in picked})

    print(f"메일 대상 로봇: {alerted or '없음 (이상이 이어지는 중 - 쿨다운)'}")

    if picked:
        recipients = [
            x.strip()
            for x in Variable.get(
                ALERT_RECIPIENTS_VARIABLE, default_var=""
            ).split(",")
            if x.strip()
        ]

        if not recipients:
            # 이력을 남기지 않으므로, 받는 사람을 등록하면 다음 실행에서 발송된다.
            print(
                f"[알람 보류] Variable '{ALERT_RECIPIENTS_VARIABLE}' 에 "
                "받는 사람이 없습니다."
            )
            return 0

        subject, body = build_alert_mail(picked)

        dry_run = Variable.get(
            ALERT_DRY_RUN_VARIABLE, default_var="false"
        ).lower() == "true"

        if dry_run:
            # 메일도 이력도 남기지 않는다 (같은 이벤트로 계속 시험해 볼 수 있음)
            print(f"[DRY_RUN] 받는 사람: {recipients}")
            print(f"[DRY_RUN] 제목: {subject}")
            print(body)
            return len(picked)

        # 실패하면 예외 -> 아래 이력 기록이 실행되지 않아 다음 실행에서 재시도
        send_alert_mail(subject, body, recipients)
        print(f"알람 메일 발송 완료: {recipients}")

    # --------------------------------------------------
    # 4-6. 새 이벤트를 전부 이력에 기록 (메일을 보낸 로봇은 MAIL_SENT = TRUE)
    #      이 태스크는 dbt 이후, 같은 DAG 안에서만 돌고 max_active_runs=1 이므로
    #      4-3에서 조회한 것과 같은 이벤트 집합이 기록된다.
    # --------------------------------------------------
    if alerted:
        robot_list = ", ".join("'" + r.replace("'", "''") + "'" for r in alerted)
        sent_expr = f"c.ROBOT_ID IN ({robot_list})"
    else:
        sent_expr = "FALSE"

    hook.run(f"""
        INSERT INTO {ALERT_LOG_TABLE}
            (EVENT_ID, ROBOT_ID, STATUS, EVENT_TIMESTAMP, MAIL_SENT)
        SELECT c.EVENT_ID, c.ROBOT_ID, c.STATUS, c.EVENT_TIMESTAMP, {sent_expr}
        FROM {CLEAN_TABLE} AS c
        LEFT JOIN {ALERT_LOG_TABLE} AS l
            ON c.EVENT_ID = l.EVENT_ID
        WHERE c.STATUS IN ('WARNING', 'DANGER')
          AND l.EVENT_ID IS NULL
    """)

    print(f"알림 이력 기록: {len(new_events)}건")
    return len(picked)


# --------------------------------------------------
# 5. DAG 정의: E -> L(RAW) -> T(dbt) -> 알람
# --------------------------------------------------

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# dbt build = 모델 실행(stg 뷰, CLEAN 증분 MERGE) + 품질 테스트 + 결과 기록(on-run-end)
# 오래된 파일을 backfill_keys로 다시 넣었다면, 실행 conf에 {"dbt_full_refresh": true}를
# 함께 주면 CLEAN을 RAW 전체 기준으로 다시 만든다. (증분 모델은 최근 30분만 다시 보기 때문)
# 프로젝트 폴더는 Windows와 컨테이너가 같이 쓰므로, 컨테이너의 실행 결과물(target, logs)은
# 컨테이너 안 /tmp에 따로 둔다. (Windows에서 만든 파싱 캐시를 컨테이너가 읽다가 깨지는 문제 방지)
DBT_BUILD_COMMAND = (
    f"{DBT_BIN} build"
    f" --project-dir {DBT_PROJECT_DIR}"
    f" --profiles-dir {DBT_PROFILES_DIR}"
    " --target-path /tmp/dbt_target"
    " --log-path /tmp/dbt_logs"
    " --no-partial-parse"
    " {{ '--full-refresh' if (dag_run.conf or {}).get('dbt_full_refresh') else '' }}"
)

with DAG(
    dag_id="robot_sensor_s3_download",
    default_args=default_args,
    description=(
        "Incremental Robot Sensor pipeline: "
        "S3 -> RAW (Airflow) -> CLEAN (dbt) + anomaly alert mail"
    ),
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026, 1, 1, tz="Asia/Seoul"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["robot", "s3", "snowflake", "dbt"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_s3_files",
        python_callable=extract_s3_files,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_snowflake",
        python_callable=load_raw_snowflake,
    )

    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command=DBT_BUILD_COMMAND,
        # 테스트 실패는 다시 돌려도 결과가 같으므로 재시도는 1번만
        # (네트워크 오류 같은 일시적 실패만 한 번 더 시도)
        retries=1,
    )

    detect_task = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
    )

    extract_task >> load_raw_task >> dbt_build_task >> detect_task
