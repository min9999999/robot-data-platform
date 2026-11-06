# -*- coding: utf-8 -*-
"""
Robot Condition Monitor

화면 구조 (스크롤 없이 한 화면)
┌──────────────────────────────────────────────────────────┐
│ 제목 · 마지막 데이터 시각         [품질 배지] [새로고침] │
├───────────────┬──────────────────────────────────────────┤
│ 로봇별 현재 상태│ 센서 추이   [항목] [기간] [로봇 선택]      │
│  ROBOT_01      │ ┌────────────────────────────────────┐ │
│  ROBOT_02      │ │            꺾은선 그래프             │ │
│  ROBOT_03      │ └────────────────────────────────────┘ │
│  ROBOT_04      │ 경고 이력                    [상태 필터] │
│  ROBOT_05      │ ┌────────────────────────────────────┐ │
│  ROBOT_06      │ │            최신 경고 N건             │ │
│                │ └────────────────────────────────────┘ │
└───────────────┴──────────────────────────────────────────┘
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, time, timedelta


# =========================================================
# 0. 설정값 (화면 크기가 안 맞으면 여기만 조정)
# =========================================================
TABLE = "DEV.CLEAN.ROBOT_SENSOR_SIMULATED"
QUALITY_TABLE = "DEV.CLEAN.DBT_TEST_RESULTS"   # dbt 테스트 결과 이력 (dbt on-run-end 훅이 기록)
QUALITY_HISTORY_RUNS = 12                      # 품질 팝업에 보여줄 최근 실행 수 (5분 간격 → 약 1시간)
LOAD_LIMIT = 5000          # 그래프용으로 불러올 최근 행 수 (로봇 6대 기준 약 83분)

# 화면 높이 (1920x1080 브라우저 100% 기준으로 한 화면에 들어오는 값)
#   노트북처럼 화면이 낮으면 CHART_HEIGHT를 220~260, ALERT_ROWS를 3으로 줄이세요.
CHART_HEIGHT = 340         # 센서 추이 그래프 높이(px)
ALERT_ROWS = 4             # 경고 이력에 보여줄 행 수
ALERT_TABLE_HEIGHT = 35 * (ALERT_ROWS + 1) + 3
LEFT_LIST_HEIGHT = CHART_HEIGHT + ALERT_TABLE_HEIGHT + 96   # 왼쪽 카드 영역: 오른쪽과 아래쪽 끝을 맞춤

HIDE_STREAMLIT_CHROME = True   # 상단 헤더/푸터를 숨겨 세로 공간 확보

# 시간 표시: 시뮬레이터는 UTC로 기록합니다. 화면은 한국 시간으로 보여줍니다.
#   Snowflake 값이 이미 한국 시간이면 SOURCE_TZ = None 으로 바꾸세요.
SOURCE_TZ = "UTC"
DISPLAY_TZ = "Asia/Seoul"

METRICS = {
    "온도": "TEMPERATURE",
    "진동": "VIBRATION",
    "모터 전류": "MOTOR_CURRENT",
    "속도": "SPEED",
}

# 시뮬레이터의 경고 기준 (카드 숫자 빨간색 표시, 그래프 점선에 사용)
THRESHOLDS = {"TEMPERATURE": 60, "VIBRATION": 0.7, "MOTOR_CURRENT": 12}

# 그래프에서 기준값 이상인 지점에 값 라벨을 붙일 때, 로봇별로 표시할 최대 개수
#   1 = 로봇별 최고점 1개만 / 숫자를 올리면 더 많이 표시 (많을수록 그래프가 복잡해짐)
MAX_LABELS_PER_ROBOT = 3

# 값이 낮아진 지점 라벨 (파란색, 점 아래쪽에 표시)
#   LOW_THRESHOLDS에 기준을 넣으면 "그 값 이하"인 지점만, 넣지 않은 항목은 로봇별 최저점을 표시합니다.
#   예) 속도가 일정 값 아래로 떨어지면 표시: {"SPEED": 0.5}
LOW_THRESHOLDS = {}
MAX_LOW_LABELS_PER_ROBOT = 1   # 로봇별로 표시할 최저점 개수

# 항목별 라벨 소수점 자리수
LABEL_DIGITS = {"TEMPERATURE": 1, "VIBRATION": 2, "MOTOR_CURRENT": 2, "SPEED": 2}

STATUS_STYLE = {
    "NORMAL": ("정상", "#2eaf7d"),
    "WARNING": ("경고", "#f59e0b"),
    "DANGER": ("위험", "#e5484d"),
}

ROBOT_COLORS = ["#2bc4ae", "#7c6ce0", "#f2784b", "#3d8bff", "#e0a800", "#8a94a6"]

PERIODS = {
    "최근 15분": 15,
    "최근 30분": 30,
    "최근 1시간": 60,
    "불러온 전체": None,
    "직접 지정": "custom",
}
DEFAULT_PERIOD_INDEX = 2   # 최근 1시간


# =========================================================
# 1. 페이지 설정
# =========================================================
st.set_page_config(
    page_title="Robot Condition Monitor",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# =========================================================
# 2. CSS
# =========================================================
CSS = """
<style>
    .block-container {
        max-width: 100%;
        padding-top: 0.9rem;
        padding-bottom: 0.3rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }

    /* 제목 */
    .app-title   { font-size: 1.5rem; font-weight: 800; line-height: 1.2; margin: 0; }
    .app-caption { font-size: 0.78rem; opacity: 0.6; margin-top: 2px; }

    /* 섹션 제목 */
    .sec-title { font-size: 1.02rem; font-weight: 700; line-height: 1.3; padding: 6px 0 0 0; }
    .sec-sub   { font-size: 0.72rem; font-weight: 400; opacity: 0.6; margin-left: 8px; }

    /* 왼쪽 로봇 카드: 영역 높이를 6등분해서 꽉 채움 */
    .robot-list {
        display: flex;
        flex-direction: column;
        gap: 6px;
        width: 100%;
        height: __LEFT_H__px;
    }
    .robot-card {
        flex: 1 1 0;
        min-height: 0;
        box-sizing: border-box;
        padding: 6px 10px;
        border: 1px solid rgba(128, 128, 128, 0.35);
        border-left: 5px solid var(--c);
        border-radius: 8px;
        background: rgba(128, 128, 128, 0.07);
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        overflow: hidden;
    }
    .rc-top  { display: flex; align-items: center; justify-content: space-between; }
    .rc-name { font-size: 0.95rem; font-weight: 800; white-space: nowrap; }
    .rc-badge {
        font-size: 0.72rem; font-weight: 700; color: #fff; background: var(--c);
        padding: 1px 9px; border-radius: 10px; white-space: nowrap;
    }
    .rc-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1px 14px; }
    .rc-item { display: flex; justify-content: space-between; font-size: 0.78rem; white-space: nowrap; }
    .rc-item span { opacity: 0.65; }
    .rc-item b    { font-variant-numeric: tabular-nums; }
    .rc-item b.over { color: #e5484d; }
    .rc-time { font-size: 0.68rem; opacity: 0.55; }

    @media (max-width: 1100px) {
        .rc-item { font-size: 0.7rem; }
        .rc-name { font-size: 0.85rem; }
    }
</style>
"""

HIDE_CHROME_CSS = """
<style>
    header[data-testid="stHeader"], #MainMenu, footer { display: none; }
</style>
"""

st.markdown(
    CSS.replace("__LEFT_H__", str(LEFT_LIST_HEIGHT))
    + (HIDE_CHROME_CSS if HIDE_STREAMLIT_CHROME else ""),
    unsafe_allow_html=True,
)


# =========================================================
# 3. Snowflake 연결
# =========================================================
conn = st.connection("snowflake")


# =========================================================
# 4. 데이터 조회
# =========================================================
@st.cache_data(ttl=300)
def load_latest_status():
    sql = f"""
        SELECT
            ROBOT_ID, EVENT_TIMESTAMP,
            TEMPERATURE, VIBRATION, MOTOR_CURRENT, SPEED, STATUS
        FROM {TABLE}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ROBOT_ID
            ORDER BY EVENT_TIMESTAMP DESC
        ) = 1
        ORDER BY ROBOT_ID
    """
    return conn.query(sql, ttl=300)


@st.cache_data(ttl=300)
def load_recent_data():
    sql = f"""
        SELECT
            EVENT_ID, ROBOT_ID, EVENT_TIMESTAMP,
            TEMPERATURE, VIBRATION, MOTOR_CURRENT, SPEED, STATUS
        FROM {TABLE}
        ORDER BY EVENT_TIMESTAMP DESC
        LIMIT {LOAD_LIMIT}
    """
    return conn.query(sql, ttl=300)


@st.cache_data(ttl=300)
def load_alert_history():
    sql = f"""
        SELECT
            ROBOT_ID, EVENT_TIMESTAMP,
            TEMPERATURE, VIBRATION, MOTOR_CURRENT, SPEED, STATUS
        FROM {TABLE}
        WHERE UPPER(STATUS) IN ('WARNING', 'DANGER')
        ORDER BY EVENT_TIMESTAMP DESC
        LIMIT 100
    """
    return conn.query(sql, ttl=300)


@st.cache_data(ttl=300)
def load_quality_latest():
    """가장 최근 dbt 실행 1회의 테스트별 결과"""
    sql = f"""
        SELECT TEST_NAME, STATUS, FAILURES, RUN_AT
        FROM {QUALITY_TABLE}
        WHERE INVOCATION_ID = (
            SELECT INVOCATION_ID
            FROM {QUALITY_TABLE}
            ORDER BY RUN_AT DESC
            LIMIT 1
        )
        ORDER BY TEST_NAME
    """
    return conn.query(sql, ttl=300)


@st.cache_data(ttl=300)
def load_quality_history():
    """최근 N회 dbt 실행별 요약 (통과/경고/실패 수, 위반 건수 합계)"""
    sql = f"""
        SELECT
            MAX(RUN_AT) AS RUN_AT,
            COUNT(*) AS TOTAL,
            COUNT_IF(LOWER(STATUS) = 'pass') AS PASSED,
            COUNT_IF(LOWER(STATUS) = 'warn') AS WARNED,
            COUNT_IF(LOWER(STATUS) IN ('fail', 'error')) AS FAILED,
            SUM(COALESCE(FAILURES, 0)) AS VIOLATIONS
        FROM {QUALITY_TABLE}
        GROUP BY INVOCATION_ID
        ORDER BY RUN_AT DESC
        LIMIT {QUALITY_HISTORY_RUNS}
    """
    return conn.query(sql, ttl=300)


# =========================================================
# 5. 데이터 정리
# =========================================================
def normalize_timestamp(series):
    """시간대 정보가 없으면 SOURCE_TZ(UTC)로 보고 DISPLAY_TZ(한국)로 바꾼 뒤 시간대 표시를 뗀다."""
    result = pd.to_datetime(series, errors="coerce")

    try:
        if result.dt.tz is None:
            if SOURCE_TZ:
                result = (
                    result.dt.tz_localize(SOURCE_TZ)
                    .dt.tz_convert(DISPLAY_TZ)
                    .dt.tz_localize(None)
                )
        else:
            result = result.dt.tz_convert(DISPLAY_TZ).dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass

    return result


def format_number(value, digits=2):
    value = pd.to_numeric(value, errors="coerce")
    if pd.isna(value):
        return "-"
    return f"{value:.{digits}f}"


def prepare_dataframe(df):
    df = df.copy()
    df.columns = [str(c).upper() for c in df.columns]

    if "EVENT_TIMESTAMP" in df.columns:
        df["EVENT_TIMESTAMP"] = normalize_timestamp(df["EVENT_TIMESTAMP"])

    for col in ["TEMPERATURE", "VIBRATION", "MOTOR_CURRENT", "SPEED"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =========================================================
# 6. 화면 조각
# =========================================================
QUALITY_LABEL = {
    "pass": "✅ 통과",
    "warn": "🟠 경고",
    "fail": "🔴 실패",
    "error": "🔴 오류",
}


def quality_summary(latest_q):
    """품질 배지 문구와 최근 실행의 테스트별 표를 만든다."""
    if latest_q is None or latest_q.empty:
        return "⚪ 품질 결과 없음", None

    q = latest_q.copy()
    q["STATUS"] = q["STATUS"].astype(str).str.lower()

    total = len(q)
    passed = int((q["STATUS"] == "pass").sum())
    warned = int((q["STATUS"] == "warn").sum())
    failed = int(q["STATUS"].isin(["fail", "error"]).sum())

    run_at = q["RUN_AT"].max()
    time_text = run_at.strftime("%H:%M") if pd.notna(run_at) else "-"

    if failed:
        label = f"🔴 품질 실패 {failed}건 · {time_text}"
    elif warned:
        label = f"🟠 품질 경고 {warned}건 · {time_text}"
    else:
        label = f"🟢 품질 {passed}/{total} 통과 · {time_text}"

    table = pd.DataFrame({
        "테스트": q["TEST_NAME"],
        "결과": q["STATUS"].map(QUALITY_LABEL).fillna(q["STATUS"]),
        "위반 건수": pd.to_numeric(q["FAILURES"], errors="coerce").fillna(0).astype(int),
    })
    return label, table


def quality_history_table(hist):
    """최근 실행별 요약 표 (최신이 위)"""
    if hist is None or hist.empty:
        return None

    h = hist.copy()
    for col in ["TOTAL", "PASSED", "WARNED", "FAILED", "VIOLATIONS"]:
        h[col] = pd.to_numeric(h[col], errors="coerce").fillna(0).astype(int)

    return pd.DataFrame({
        "실행 시각": h["RUN_AT"].dt.strftime("%m-%d %H:%M"),
        "통과": h["PASSED"].astype(str) + "/" + h["TOTAL"].astype(str),
        "경고": h["WARNED"],
        "실패": h["FAILED"],
        "위반 건수": h["VIOLATIONS"],
    })


def render_quality_badge(container):
    """제목 줄 오른쪽의 품질 배지. 누르면 결과표가 열린다.
    조회에 실패해도 대시보드 나머지는 그대로 보이도록 따로 감싼다."""
    try:
        latest_q = prepare_dataframe(load_quality_latest())
        hist = prepare_dataframe(load_quality_history())
        for df in (latest_q, hist):
            if "RUN_AT" in df.columns:
                df["RUN_AT"] = normalize_timestamp(df["RUN_AT"])
    except Exception:
        latest_q, hist = None, None

    label, table = quality_summary(latest_q)
    history = quality_history_table(hist)

    with container:
        with st.popover(label, use_container_width=True):
            st.markdown("**최근 dbt 품질 테스트 결과**")
            if table is None:
                st.info("아직 기록된 테스트 결과가 없습니다.")
            else:
                st.dataframe(table, hide_index=True, use_container_width=True)

            if history is not None:
                st.markdown(f"**최근 {len(history)}회 실행 추이**")
                st.dataframe(history, hide_index=True, use_container_width=True)

            st.caption(
                "staging 테스트(원본 null 검사)는 경고, "
                "CLEAN 테스트(중복·상태값)는 실패로 처리됩니다."
            )

def robot_cards_html(latest):
    """왼쪽: 로봇 6대 상태 카드 (HTML 한 덩어리)"""
    cards = []

    for _, robot in latest.iterrows():
        status = str(robot.get("STATUS", "")).upper()
        label, color = STATUS_STYLE.get(status, ("미확인", "#8a94a6"))

        ts = robot.get("EVENT_TIMESTAMP")
        ts_text = ts.strftime("%m-%d %H:%M:%S") if pd.notna(ts) else "-"

        def cell(name, col, digits):
            value = pd.to_numeric(robot.get(col), errors="coerce")
            limit = THRESHOLDS.get(col)
            over = (
                ' class="over"'
                if limit is not None and pd.notna(value) and value >= limit
                else ""
            )
            return (
                f'<div class="rc-item"><span>{name}</span>'
                f"<b{over}>{format_number(value, digits)}</b></div>"
            )

        cards.append(
            f'<div class="robot-card" style="--c:{color}">'
            f'<div class="rc-top">'
            f'<span class="rc-name">🤖 {robot["ROBOT_ID"]}</span>'
            f'<span class="rc-badge">{label}</span></div>'
            f'<div class="rc-grid">'
            f'{cell("온도", "TEMPERATURE", 1)}'
            f'{cell("진동", "VIBRATION", 2)}'
            f'{cell("전류", "MOTOR_CURRENT", 2)}'
            f'{cell("속도", "SPEED", 2)}'
            f"</div>"
            f'<div class="rc-time">{ts_text}</div></div>'
        )

    return '<div class="robot-list">' + "".join(cards) + "</div>"


def pick_time_window(period_label, data):
    """기간 선택 → (시작, 끝). 기준 시각은 '지금'이 아니라 불러온 데이터의 마지막 시각."""
    minutes = PERIODS[period_label]
    first_ts = data["EVENT_TIMESTAMP"].min()
    last_ts = data["EVENT_TIMESTAMP"].max()

    if isinstance(minutes, int):
        return last_ts - timedelta(minutes=minutes), last_ts, None

    if minutes is None:
        return first_ts, last_ts, None

    # 직접 지정: 날짜·시간 입력칸을 그래프 위에 한 줄 더 보여준다.
    c1, c2, c3, c4 = st.columns([1.15, 1.15, 0.8, 0.8])
    with c1:
        start_date = st.date_input(
            "시작 날짜", value=last_ts.date(), key="start_date"
        )
    with c2:
        end_date = st.date_input(
            "종료 날짜", value=last_ts.date(), key="end_date"
        )
    with c3:
        start_time = st.time_input("시작 시간", value=time(0, 0), key="start_time")
    with c4:
        end_time = st.time_input("종료 시간", value=time(23, 59), key="end_time")

    if start_date > end_date or (start_date == end_date and start_time > end_time):
        return None, None, "시작 시각은 종료 시각보다 늦을 수 없습니다."

    return (
        pd.Timestamp(datetime.combine(start_date, start_time)),
        pd.Timestamp(datetime.combine(end_date, end_time)),
        None,
    )


def build_chart(chart_data, metric, metric_label, robot_ids):
    spans_days = (
        chart_data["EVENT_TIMESTAMP"].dt.date.nunique() > 1
    )
    x_format = "%m-%d %H:%M" if spans_days else "%H:%M"

    color_enc = alt.Color(
        "ROBOT_ID:N",
        title=None,
        scale=alt.Scale(
            domain=robot_ids,
            range=ROBOT_COLORS[: len(robot_ids)],
        ),
        legend=alt.Legend(
            orient="top",
            direction="horizontal",
            labelFontSize=11,
            symbolStrokeWidth=3,
        ),
    )
    x_enc = alt.X(
        "EVENT_TIMESTAMP:T",
        title=None,
        axis=alt.Axis(format=x_format, labelAngle=0, grid=False),
    )
    y_enc = alt.Y(
        f"{metric}:Q",
        title=metric_label,
        scale=alt.Scale(zero=False),
    )

    lines = (
        alt.Chart(chart_data)
        .mark_line(strokeWidth=1.8)
        .encode(
            x=x_enc,
            y=y_enc,
            color=color_enc,
            tooltip=[
                alt.Tooltip("ROBOT_ID:N", title="로봇"),
                alt.Tooltip(
                    "EVENT_TIMESTAMP:T",
                    title="시간",
                    format="%Y-%m-%d %H:%M:%S",
                ),
                alt.Tooltip(f"{metric}:Q", title=metric_label, format=".2f"),
            ],
        )
    )

    layers = [lines]

    limit = THRESHOLDS.get(metric)

    # 경고 기준선: 데이터가 기준에 가까워졌을 때만 그린다 (평소엔 그래프가 눌리지 않게)
    if limit is not None and chart_data[metric].max() >= limit * 0.85:
        rule = (
            alt.Chart(pd.DataFrame({"limit": [limit]}))
            .mark_rule(strokeDash=[5, 4], color="#e5484d", strokeWidth=1.2)
            .encode(y="limit:Q")
        )
        layers.append(rule)

    # 기준값 이상인 지점: 점 + 값 라벨
    high_idx = []
    if limit is not None:
        high = chart_data[chart_data[metric] >= limit].copy()

        if not high.empty:
            # 라벨이 너무 많으면 지저분하므로 로봇별 상위 N개만 표시
            high = (
                high.sort_values(metric, ascending=False)
                .groupby("ROBOT_ID", group_keys=False)
                .head(MAX_LABELS_PER_ROBOT)
            )
            digits = LABEL_DIGITS.get(metric, 1)

            points = (
                alt.Chart(high)
                .mark_point(filled=True, size=70, stroke="white", strokeWidth=1)
                .encode(x=x_enc, y=y_enc, color=color_enc)
            )
            labels = (
                alt.Chart(high)
                .mark_text(
                    dy=-11,
                    fontSize=11,
                    fontWeight="bold",
                    color="#e5484d",
                )
                .encode(
                    x=x_enc,
                    y=y_enc,
                    text=alt.Text(f"{metric}:Q", format=f".{digits}f"),
                )
            )
            layers.extend([points, labels])
            high_idx = list(high.index)

    # 값이 낮아진 지점: 점 + 값 라벨 (파란색, 점 아래쪽)
    low_limit = LOW_THRESHOLDS.get(metric)
    low = chart_data if low_limit is None else chart_data[chart_data[metric] <= low_limit]
    low = (
        low.drop(index=high_idx, errors="ignore")
        .sort_values(metric, ascending=True)
        .groupby("ROBOT_ID", group_keys=False)
        .head(MAX_LOW_LABELS_PER_ROBOT)
    )

    if not low.empty:
        low_digits = LABEL_DIGITS.get(metric, 1)

        low_points = (
            alt.Chart(low)
            .mark_point(filled=True, size=70, stroke="white", strokeWidth=1)
            .encode(x=x_enc, y=y_enc, color=color_enc)
        )
        low_labels = (
            alt.Chart(low)
            .mark_text(
                dy=13,
                fontSize=11,
                fontWeight="bold",
                color="#3d8bff",
            )
            .encode(
                x=x_enc,
                y=y_enc,
                text=alt.Text(f"{metric}:Q", format=f".{low_digits}f"),
            )
        )
        layers.extend([low_points, low_labels])

    chart = alt.layer(*layers) if len(layers) > 1 else lines
    return chart.properties(height=CHART_HEIGHT).interactive()


# =========================================================
# 7. 대시보드
# =========================================================
@st.fragment(run_every="5m")
def dashboard():

    # -----------------------------------------------------
    # 맨 윗줄: 제목 + 새로고침
    # -----------------------------------------------------
    title_col, quality_col, button_col = st.columns([4, 1.6, 1])

    with button_col:
        if st.button("🔄 새로고침", use_container_width=True):
            load_latest_status.clear()
            load_recent_data.clear()
            load_alert_history.clear()
            load_quality_latest.clear()
            load_quality_history.clear()

    render_quality_badge(quality_col)

    header_slot = title_col.empty()

    # -----------------------------------------------------
    # 데이터 불러오기
    # -----------------------------------------------------
    try:
        latest = prepare_dataframe(load_latest_status())
        data = prepare_dataframe(load_recent_data())
        alerts = prepare_dataframe(load_alert_history())

    except Exception as e:
        header_slot.markdown(
            '<div class="app-title">🤖 Robot Condition Monitor</div>',
            unsafe_allow_html=True,
        )
        st.error("Snowflake 데이터 조회에 실패했습니다.")
        st.exception(e)
        return

    last_ts = data["EVENT_TIMESTAMP"].max() if not data.empty else pd.NaT
    last_text = (
        f" · 마지막 데이터 {last_ts.strftime('%m-%d %H:%M:%S')}"
        if pd.notna(last_ts)
        else ""
    )
    header_slot.markdown(
        '<div class="app-title">🤖 Robot Condition Monitor</div>'
        '<div class="app-caption">'
        f"AWS S3 → Airflow → Snowflake → dbt → Streamlit{last_text}"
        "</div>",
        unsafe_allow_html=True,
    )

    if latest.empty:
        st.warning("표시할 로봇 데이터가 없습니다.")
        return

    latest["ROBOT_ID"] = latest["ROBOT_ID"].astype(str)
    latest = latest.sort_values("ROBOT_ID").head(6)

    # -----------------------------------------------------
    # 메인: 왼쪽 상태 카드 / 오른쪽 (추이 + 경고 이력)
    # -----------------------------------------------------
    left_col, right_col = st.columns([1, 3], gap="medium")

    # =====================================================
    # 왼쪽: 로봇별 현재 상태
    # =====================================================
    with left_col:
        st.markdown(
            '<div class="sec-title">📡 로봇별 현재 상태</div>',
            unsafe_allow_html=True,
        )
        st.markdown(robot_cards_html(latest), unsafe_allow_html=True)

    # =====================================================
    # 오른쪽 위: 센서 추이
    # =====================================================
    with right_col:
        t_col, metric_col, period_col, robot_col = st.columns(
            [1.25, 0.9, 1.1, 2.3]
        )

        available_robots = (
            sorted(data["ROBOT_ID"].dropna().astype(str).unique())
            if not data.empty
            else []
        )

        with metric_col:
            metric_label = st.selectbox(
                "센서 항목",
                list(METRICS.keys()),
                key="metric_select",
                label_visibility="collapsed",
            )

        with period_col:
            period_label = st.selectbox(
                "기간",
                list(PERIODS.keys()),
                index=DEFAULT_PERIOD_INDEX,
                key="period_select",
                label_visibility="collapsed",
            )

        with robot_col:
            selected_robots = st.multiselect(
                "로봇 선택",
                options=available_robots,
                default=available_robots[:6],
                placeholder="로봇 선택",
                key="robot_select",
                label_visibility="collapsed",
            )

        metric = METRICS[metric_label]
        count_text = ""
        message = None
        chart = None

        if data.empty:
            message = ("info", "센서 데이터가 없습니다.")

        elif not selected_robots:
            message = ("info", "그래프에 표시할 로봇을 선택해 주세요.")

        else:
            start_dt, end_dt, error = pick_time_window(period_label, data)

            if error:
                message = ("warning", error)
            else:
                chart_data = data[
                    data["ROBOT_ID"].astype(str).isin(selected_robots)
                    & data["EVENT_TIMESTAMP"].notna()
                    & (data["EVENT_TIMESTAMP"] >= start_dt)
                    & (data["EVENT_TIMESTAMP"] <= end_dt)
                ].dropna(subset=[metric])

                if chart_data.empty:
                    message = (
                        "info",
                        "선택한 기간의 데이터가 없습니다. 기간을 넓혀 주세요.",
                    )
                else:
                    chart_data = chart_data.sort_values("EVENT_TIMESTAMP")
                    chart = build_chart(
                        chart_data,
                        metric,
                        metric_label,
                        [r for r in available_robots if r in selected_robots],
                    )
                    count_text = (
                        f'<span class="sec-sub">{len(chart_data):,}건</span>'
                    )

        with t_col:
            st.markdown(
                f'<div class="sec-title">📈 센서 추이{count_text}</div>',
                unsafe_allow_html=True,
            )

        if chart is not None:
            st.altair_chart(chart, use_container_width=True)
        elif message:
            getattr(st, message[0])(message[1])

        # =================================================
        # 오른쪽 아래: 경고 이력
        # =================================================
        a_title_col, a_filter_col = st.columns([1.6, 1])

        filtered_alerts = alerts.iloc[0:0]

        if not alerts.empty:
            alerts["STATUS"] = alerts["STATUS"].astype(str).str.upper()

            with a_filter_col:
                selected_statuses = st.multiselect(
                    "상태 필터",
                    options=["WARNING", "DANGER"],
                    default=["WARNING", "DANGER"],
                    format_func=lambda v: {
                        "WARNING": "🟠 경고",
                        "DANGER": "🔴 위험",
                    }.get(v, v),
                    key="alert_filter",
                    label_visibility="collapsed",
                )

            filtered_alerts = alerts[
                alerts["STATUS"].isin(selected_statuses)
            ].copy()

        with a_title_col:
            st.markdown(
                '<div class="sec-title">🚨 경고 이력'
                f'<span class="sec-sub">{len(filtered_alerts):,}건 중 '
                f"최신 {ALERT_ROWS}건</span></div>",
                unsafe_allow_html=True,
            )

        if alerts.empty:
            st.info("조회된 경고 및 위험 이력이 없습니다.")

        elif filtered_alerts.empty:
            st.info("선택한 상태의 경고 이력이 없습니다.")

        else:
            display = filtered_alerts.head(ALERT_ROWS).copy()

            display["EVENT_TIMESTAMP"] = display["EVENT_TIMESTAMP"].dt.strftime(
                "%m-%d %H:%M:%S"
            )
            display["STATUS"] = display["STATUS"].map(
                {"WARNING": "🟠 경고", "DANGER": "🔴 위험"}
            ).fillna(display["STATUS"])

            display = display.rename(
                columns={
                    "ROBOT_ID": "로봇",
                    "EVENT_TIMESTAMP": "발생 시간",
                    "STATUS": "상태",
                    "TEMPERATURE": "온도",
                    "VIBRATION": "진동",
                    "MOTOR_CURRENT": "모터 전류",
                    "SPEED": "속도",
                }
            )

            columns = ["로봇", "발생 시간", "상태", "온도", "진동", "모터 전류", "속도"]
            display = display[[c for c in columns if c in display.columns]]

            st.dataframe(
                display,
                use_container_width=True,
                hide_index=True,
                height=ALERT_TABLE_HEIGHT,
                column_config={
                    "온도": st.column_config.NumberColumn("온도", format="%.1f"),
                    "진동": st.column_config.NumberColumn("진동", format="%.2f"),
                    "모터 전류": st.column_config.NumberColumn(
                        "모터 전류", format="%.2f"
                    ),
                    "속도": st.column_config.NumberColumn("속도", format="%.2f"),
                },
            )


# =========================================================
# 8. 앱 실행
# =========================================================
dashboard()