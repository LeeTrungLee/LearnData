import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import json
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})
seen_links = set()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered: {msg.value().decode('utf-8')} to {msg.topic()} [{msg.partition()}]")

def scrape_articles():
    url = "https://thanhnien.vn/"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    results = []
    for item in soup.find_all("div", class_="box-category-item"):
        a = item.find("a")
        if a:
            title = a.get_text(strip=True)
            link = a.get("href", "")
            if not link.startswith("http"):
                link = "https://thanhnien.vn" + link
            if link not in seen_links:
                seen_links.add(link)
                results.append({
                    "title": title,
                    "link": link,
                    "description": "",
                    "timestamp": time.time()
                })
    return results


while True:
    print("Bắt đầu crawl...")
    articles = scrape_articles()
    print(f"Tìm được {len(articles)} bài mới.")
    for article in articles:
        producer.produce("web_data", json.dumps(article).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
        print(f"Sent: {article['title']}")
    producer.flush()
    time.sleep(30)
