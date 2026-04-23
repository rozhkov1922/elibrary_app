def parse_journals(author_id):
    import requests
    import re
    import time

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
        raise Exception("Ошибка загрузки страницы")

    html = response.text

    # =========================
    # ПАРСИНГ REGEX
    # =========================

    pattern = re.compile(
        r'<tr id="title_(\d+)".*?>.*?<td.*?>(.*?)</td>',
        re.DOTALL
    )

    matches = pattern.findall(html)

    data = []

    for rinc_id, raw_text in matches:
        try:
            text = re.sub(r"<.*?>", "", raw_text).strip()

            match = re.match(r"(.+?)\s*\((\d+)\)", text)

            if match:
                data.append({
                    "author_id": author_id,
                    "rinc_id": int(rinc_id),
                    "journal": match.group(1),
                    "publications": int(match.group(2))
                })
        except:
            continue

    df = pd.DataFrame(data)

    if df.empty:
        raise Exception("Не удалось распарсить данные (возможна блокировка)")

    return df
