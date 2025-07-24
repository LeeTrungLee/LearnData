import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils.logger import get_logger

logger = get_logger(__name__)

def scrape_website(url: str) -> pd.DataFrame:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"❌ Không thể truy cập {url}: {e}")
        return pd.DataFrame()

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    articles = soup.find_all("div", class_="box-category-content")
    for article in articles:
        a_tag = article.find("a", href=True)
        if not a_tag:
            continue

        # Lấy title từ text nếu có
        title = a_tag.get_text(strip=True) or a_tag.get("title") or "Không có tiêu đề"

        # Lấy description từ thuộc tính title, hoặc từ nội dung trong article
        description = a_tag.get("title")
        if not description:
            # Tìm đoạn văn đầu tiên trong article
            desc_tag = article.find("p")
            if desc_tag:
                description = desc_tag.get_text(strip=True)
            else:
                description = ""

        link = urljoin(url, a_tag["href"])

        results.append({
            "title": title,
            "link": link,
            "description": description,
        })

    logger.info(f"✅ Đã cào {len(results)} phần tử từ {url}")
    return pd.DataFrame(results)
