import streamlit as st
import pandas as pd
import re
import random
import time
import requests

from bs4 import BeautifulSoup
from sqlalchemy import create_engine

# --- Selenium (используется только как fallback) ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# =========================
# UI
# =========================

st.set_page_config(page_title="RINC Parser", layout="wide")

st.title("Парсинг публикаций автора eLibrary")

st.warning("⚠️ Приложение работает только внутри корпоративной сети HSE.Work")

st.markdown(
    "Приложение получает список журналов автора из eLibrary по Author ID, "
    "сопоставляет журналы с базой НИУ ВШЭ и формирует две таблицы: "
    "список публикаций и сводную таблицу по категориям журналов."
)

author_id = st.number_input("Введите Author ID (РИНЦ)", min_value=1, step=1)
run_button = st.button("Запустить")

BASE_URL = "https://elibrary.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": BASE_URL
}

# =========================
# 1. REQUESTS (PRIMARY)
# =========================

def parse_journals_requests(author_id):
    url = f"{BASE_URL}/author_items_titles.asp?id={author_id}&show_refs=1&hide_doubles=1"

    session = requests.Session()

    for attempt in range(3):
        try:
            # прогрев
            session.get(BASE_URL, headers=HEADERS, timeout=20)
            time.sleep(1)

            response = session.get(url, headers=HEADERS, timeout=20)

            if response.status_code != 200:
                raise Exception("HTTP error")

            soup = BeautifulSoup(response.text, "html.parser")
            rows = soup.select("tr[id^='title_']")

            data = []

            for row in rows:
                try:
                    rinc_id = int(row.get("id").split("_")[1])
                    text = row.get_text(strip=True)

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
                raise Exception("Empty data")

            return df

        except Exception:
            time.sleep(2)

    raise Exception("REQUESTS_FAILED")


# =========================
# 2. SELENIUM (FALLBACK)
# =========================

def get_driver():
    options = webdriver.ChromeOptions()

    options.binary_location = "/usr/bin/google-chrome"

    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    service = Service(ChromeDriverManager().install())

    return webdriver.Chrome(service=service, options=options)


def parse_journals_selenium(author_id):

    driver = get_driver()

    try:
        URL_TEMPLATE = "https://elibrary.ru/author_items_titles.asp?id={}&show_refs=1&hide_doubles=1&rand={}"
        rand_value = f"{random.uniform(0, 0.3):.17f}"
        url = URL_TEMPLATE.format(author_id, rand_value)

        driver.get(url)

        wait = WebDriverWait(driver, 15)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "tr[id^='title_']"))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, "tr[id^='title_']")

        data = []

        for row in rows:
            try:
                rinc_id = int(row.get_attribute("id").split("_")[1])
                text = row.text.strip()

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
            raise Exception("Selenium empty")

        return df

    finally:
        driver.quit()


# =========================
# 3. SMART PARSER
# =========================

def parse_journals(author_id):

    try:
        st.info("Попытка через requests...")
        return parse_journals_requests(author_id)

    except Exception:
        st.warning("requests не сработал → пробуем Selenium...")

        try:
            return parse_journals_selenium(author_id)
        except Exception:
            raise Exception(
                "❌ Не удалось получить данные.\n\n"
                "Причины:\n"
                "- блокировка eLibrary\n"
                "- вы вне сети HSE / РФ\n"
                "- Selenium не запустился"
            )


# =========================
# DB
# =========================

@st.cache_data
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

        st.subheader("Список публикаций автора")
        st.dataframe(final_df, use_container_width=True)

        st.download_button(
            "Скачать final_df.csv",
            final_df.to_csv(index=False).encode("utf-8"),
            "final_df.csv",
            "text/csv"
        )

        st.subheader("Сводная таблица по спискам НИУ ВШЭ")
        st.dataframe(pivot_df, use_container_width=True)

        st.download_button(
            "Скачать pivot_df.csv",
            pivot_df.to_csv(index=False).encode("utf-8"),
            "pivot_df.csv",
            "text/csv"
        )

    except Exception as e:
        st.error(str(e))
