import streamlit as st
import pandas as pd
import re
import random

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from sqlalchemy import create_engine

# =========================
# STREAMLIT UI
# =========================

st.title("Парсинг публикаций автора eLibrary")

st.warning("⚠️ Приложение работает только внутри корпоративной сети HSE.Work")

st.markdown(
    "Приложение парсит список журналов автора из eLibrary по его Author ID, "
    "сопоставляет их с базой данных НИУ ВШЭ и строит сводную таблицу публикаций "
    "по категориям журналов."
)

author_id = st.number_input("Введите Author ID (РИНЦ)", min_value=1, step=1)

run_button = st.button("Запустить")

# =========================
# FUNCTIONS
# =========================

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--headless=new")
    return webdriver.Chrome(options=options)


def open_author(driver, author_id):
    URL_TEMPLATE = "https://elibrary.ru/author_items_titles.asp?id={}&order=0&selids=&show_hash=0&show_refs=1&hide_doubles=1&rand={}"
    rand_value = f"{random.uniform(0, 0.3):.17f}"
    url = URL_TEMPLATE.format(author_id, rand_value)

    driver.get(url)

    wait = WebDriverWait(driver, 15)
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "tr[id^='title_']"))
    )


def parse_journals(driver, author_id):
    rows = driver.find_elements(By.CSS_SELECTOR, "tr[id^='title_']")
    data = []

    for row in rows:
        try:
            row_id = row.get_attribute("id")
            rinc_id = int(row_id.split("_")[1])

            text = row.text.strip()
            match = re.match(r"(.+?)\s*\((\d+)\)", text)

            if match:
                journal = match.group(1).strip()
                publications = int(match.group(2))

                data.append({
                    "author_id": author_id,
                    "rinc_id": rinc_id,
                    "journal": journal,
                    "publications": publications
                })

        except Exception:
            continue

    return pd.DataFrame(data)


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
# MAIN LOGIC
# =========================

if run_button and author_id:

    with st.spinner("Сбор данных..."):

        driver = get_driver()

        try:
            # парсинг
            open_author(driver, author_id)
            df_parsed = parse_journals(driver, author_id)

            # база
            df_db = load_journal_mapping()

            # типы
            df_parsed["rinc_id"] = pd.to_numeric(df_parsed["rinc_id"], errors="coerce")
            df_db["rinc_id"] = pd.to_numeric(df_db["rinc_id"], errors="coerce")

            df_parsed = df_parsed.dropna(subset=["rinc_id"])
            df_db = df_db.dropna(subset=["rinc_id"])

            # merge
            df_merged = df_parsed.merge(df_db, on="rinc_id", how="left")

            # final
            final_df = df_merged[
                ["author_id", "rinc_id", "journal", "hse_list_2", "publications"]
            ].copy()

            # pivot
            pivot_df = final_df.pivot_table(
                index="author_id",
                columns="hse_list_2",
                values="publications",
                aggfunc="sum",
                fill_value=0
            ).reset_index()

            # =========================
            # OUTPUT
            # =========================

            st.subheader("Список публикаций автора")
            st.dataframe(final_df)

            st.download_button(
                label="Скачать final_df.csv",
                data=final_df.to_csv(index=False).encode("utf-8"),
                file_name="final_df.csv",
                mime="text/csv"
            )

            st.subheader("Сводная таблица по спискам НИУ ВШЭ")
            st.dataframe(pivot_df)

            st.download_button(
                label="Скачать pivot_df.csv",
                data=pivot_df.to_csv(index=False).encode("utf-8"),
                file_name="pivot_df.csv",
                mime="text/csv"
            )

        finally:
            driver.quit()
