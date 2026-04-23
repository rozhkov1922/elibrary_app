import streamlit as st
import pandas as pd
import re
import time
import requests

from lxml import html
from sqlalchemy import create_engine

# =========================
# UI
# =========================

st.set_page_config(page_title="RINC Parser", layout="wide")

st.title("Парсинг публикаций автора eLibrary")

st.warning("⚠️ Приложение работает только внутри корпоративной сети HSE.Work")

st.markdown(
    "Приложение получает список журналов автора из eLibrary по Author ID, "
    "извлекает количество публикаций, сопоставляет журналы с базой НИУ ВШЭ "
    "и формирует две таблицы: список публикаций автора и сводную таблицу "
    "по категориям журналов."
)

author_id = st.number_input("Введите Author ID (РИНЦ)", min_value=1, step=1)
run_button = st.button("Запустить")

# =========================
# PARSER (lxml)
# =========================

@st.cache_data(show_spinner=False)
def parse_journals(author_id):

    url = f"https://elibrary.ru/author_items_titles.asp?id={author_id}&show_refs=1&hide_doubles=1"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
        "Referer": "https://elibrary.ru"
    }

    session = requests.Session()

    # прогрев
    session.get("https://elibrary.ru", headers=headers)
    time.sleep(1)

    response = session.get(url, headers=headers, timeout=20)

    if response.status_code != 200:
        raise Exception("Ошибка загрузки страницы eLibrary")

    tree = html.fromstring(response.content)

    rows = tree.xpath("//tr[starts-with(@id, 'title_')]")

    data = []

    for row in rows:
        try:
            rinc_id = int(row.attrib["id"].split("_")[1])
            text = row.text_content().strip()

            match = re.match(r"(.+?)\s*\((\d+)\)", text)

            if match:
                data.append({
                    "author_id": author_id,
                    "rinc_id": rinc_id,
                    "journal": match.group(1),
                    "publications": int(match.group(2))
                })
        except:
            continue

    df = pd.DataFrame(data)

    if df.empty:
        raise Exception(
            "Не удалось получить данные.\n"
            "Возможна блокировка или отсутствует доступ из вашей сети."
        )

    return df

# =========================
# DB
# =========================

@st.cache_data(show_spinner=False)
def load_journal_mapping():
    engine = create_engine(
        "postgresql+psycopg2://supnc_team:_F6dq}Wg)M@192.168.206.41:5432/supnc"
    )

    query = """
    SELECT rinc_id, hse_list_2
    FROM dim_journal
    """

    return pd.read_sql(query, engine)

# =========================
# PROCESS
# =========================

def process_data(author_id):

    df_parsed = parse_journals(author_id)
    df_db = load_journal_mapping()

    df_parsed["rinc_id"] = pd.to_numeric(df_parsed["rinc_id"], errors="coerce")
    df_db["rinc_id"] = pd.to_numeric(df_db["rinc_id"], errors="coerce")

    df_parsed = df_parsed.dropna(subset=["rinc_id"])
    df_db = df_db.dropna(subset=["rinc_id"])

    df_merged = df_parsed.merge(df_db, on="rinc_id", how="left")

    final_df = df_merged[
        ["author_id", "rinc_id", "journal", "hse_list_2", "publications"]
    ].copy()

    pivot_df = final_df.pivot_table(
        index="author_id",
        columns="hse_list_2",
        values="publications",
        aggfunc="sum",
        fill_value=0
    ).reset_index()

    return final_df, pivot_df

# =========================
# RUN
# =========================

if run_button and author_id:

    try:
        with st.spinner("Обработка данных..."):
            final_df, pivot_df = process_data(author_id)

        # ===== TABLE 1 =====
        st.subheader("Список публикаций автора (final_df)")
        st.dataframe(final_df, use_container_width=True)

        st.download_button(
            "Скачать final_df.csv",
            final_df.to_csv(index=False).encode("utf-8"),
            "final_df.csv",
            "text/csv"
        )

        # ===== TABLE 2 =====
        st.subheader("Сводная таблица по спискам НИУ ВШЭ (pivot_df)")
        st.dataframe(pivot_df, use_container_width=True)

        st.download_button(
            "Скачать pivot_df.csv",
            pivot_df.to_csv(index=False).encode("utf-8"),
            "pivot_df.csv",
            "text/csv"
        )

    except Exception as e:
        st.error(str(e))
