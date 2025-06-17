import requests
import time
import os
import json
import re
import asyncio
import aiohttp
from crawl4ai import AsyncWebCrawler

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}

BASE_URL = "https://www.vivino.com/api/explore/explore"
WINE_TYPES = {"1": "Red", "2": "White"}

# Create output directories
JSON_DIR = "vivino_full_json"
IMAGE_DIR = "wine_image"
os.makedirs(JSON_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

# Price bucket generation
price_buckets = []
for start in range(0, 150, 50):
    price_buckets.append((start, start + 50))

last = 150
while last < 1000:
    next_stop = last + 100 if last < 1000 else (last + 500 if last < 5000 else last + 5000)
    price_buckets.append((last, min(next_stop, 50000)))
    last = next_stop

def save_to_json(batch_data, batch_num):
    filename = f"{JSON_DIR}/wines_batch_{batch_num}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(batch_data, f, indent=2, ensure_ascii=False)
    print(f"✅ Saved batch {batch_num} to {filename}")

async def download_image(image_url, filename):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(image_url) as response:
                if response.status == 200:
                    # Sanitize filename to avoid invalid characters
                    safe_filename = re.sub(r'[^\w\-_\.]', '_', filename)
                    filepath = os.path.join(IMAGE_DIR, safe_filename)
                    with open(filepath, 'wb') as f:
                        f.write(await response.read())
                    print(f"✅ Downloaded image to {filepath}")
                    return safe_filename
                else:
                    print(f"❌ Failed to download image {image_url}: HTTP {response.status}")
                    return None
    except Exception as e:
        print(f"❌ Error downloading image {image_url}: {e}")
        return None

async def append_data(wine_url, wine_id):
    result_data = {
        "Food Pairing": [],
        "Alcohol Content": "N/A",
        "Wine Description": "N/A",
        "Wine Image Name": "N/A"
    }

    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=wine_url)

            # Extract food pairings
            pairing_marker = "Are you cooking something else? Search for [wines by food pairings]"
            if pairing_marker in result.markdown:
                pairing_section = result.markdown.split(pairing_marker)[1]
                for item in pairing_section.split('[')[1:]:
                    if '](https://' in item:
                        food_name = item.split('](https://')[0].strip()
                        url = item.split('](https://')[1].split(')')[0]
                        if 'food-pairing' in url:
                            result_data["Food Pairing"].append(food_name)
                        else:
                            break

            # Extract alcohol content
            alcohol_marker = "Alcohol content"
            for line in result.markdown.split('\n'):
                if alcohol_marker in line:
                    parts = line.split('|')
                    if len(parts) > 1:
                        result_data["Alcohol Content"] = parts[-1].strip()
                    break

            # Extract wine description
            description_start = "### Wine description"
            description_end = "## Compare Vintages"
            if description_start in result.markdown and description_end in result.markdown:
                description_section = result.markdown.split(description_start)[1].split(description_end)[0].strip()
                cleaned_description = ' '.join(
                    line.strip().strip('|').strip() for line in description_section.split('\n') if
                    line.strip() and not line.startswith('| ---'))
                result_data["Wine Description"] = cleaned_description

            # Extract and download wine image above first '#'
            content_before_hash = result.markdown.split('#')[0]
            image_pattern = r"!\[([^\]]+)\]\((https?://[^)]+\.png)\)"
            image_match = re.search(image_pattern, content_before_hash)
            if image_match:
                wine_name = image_match.group(1).strip()
                image_url = image_match.group(2)
                # Use wine_id to ensure unique filenames
                filename = f"{wine_id}_{wine_name.replace(' ', '_')}.png"
                downloaded_filename = await download_image(image_url, filename)
                if downloaded_filename:
                    result_data["Wine Image Name"] = downloaded_filename
            else:
                print(f"Wine image not found above '#' for {wine_url}")

    except Exception as e:
        print(f"Error fetching extra data from {wine_url}: {e}")

    return result_data

def fetch_all_wines(delay=1.5):
    all_wines = {}
    batch = []
    batch_num = 1

    for wine_type_id, wine_type_name in WINE_TYPES.items():
        for min_price, max_price in price_buckets:
            print(f"\n🍷 Crawling {wine_type_name} wines in price ${min_price}–${max_price}")
            page = 1
            while True:
                params = {
                    "country_code": "US",
                    "currency_code": "USD",
                    "grape_filter": "varietal",
                    "min_rating": "0",
                    "order_by": "ratings_count",
                    "order": "desc",
                    "price_range_min": str(min_price),
                    "price_range_max": str(max_price),
                    "wine_type_ids[]": wine_type_id,
                    "language": "en",
                    "page": page
                }

                try:
                    response = requests.get(BASE_URL, headers=HEADERS, params=params)
                    response.raise_for_status()
                    data = response.json()
                    matches = data.get("explore_vintage", {}).get("matches", [])
                    if not matches:
                        break

                    new_wines = 0
                    for entry in matches:
                        vintage = entry.get("vintage", {})
                        wine = vintage.get("wine", {})
                        wine_id = wine.get("id")
                        if wine_id in all_wines:
                            continue  # Skip duplicates

                        stats = vintage.get("statistics", {})
                        region = wine.get("region", {})
                        country = region.get("country", {})
                        winery = wine.get("winery", {})

                        price_info = entry.get("price", {})
                        wine_url = f"https://www.vivino.com/US/en/{'-'.join(wine.get('name', '').lower().split())}/w/{wine_id}"

                        extra_data = asyncio.run(append_data(wine_url, wine_id))

                        wine_data = {
                            "Name": wine.get("name", "N/A"),
                            "Year": vintage.get("name", "N/A").split()[-1],
                            "Rating": stats.get("ratings_average", "N/A"),
                            "Rating Count": stats.get("ratings_count", "N/A"),
                            "Price": price_info.get("amount", "N/A"),
                            "Original Price": price_info.get("discounted_from", price_info.get("amount", "N/A")),
                            "Winery": winery.get("name", "N/A"),
                            "Region": region.get("name", "N/A"),
                            "Country": country.get("name", "N/A"),
                            "Type": wine_type_name,
                            "URL": wine_url,
                            "Food Pairing": ', '.join(extra_data.get("Food Pairing", [])),
                            "Alcohol Content": extra_data.get("Alcohol Content", "N/A"),
                            "Wine Description": extra_data.get("Wine Description", "N/A"),
                            "Wine Image Name": extra_data.get("Wine Image Name", "N/A")
                        }

                        all_wines[wine_id] = wine_data
                        batch.append(wine_data)
                        new_wines += 1

                    print(f"🔎 Page {page} — 🆕 New wines: {new_wines} | Total collected: {len(all_wines)}")

                    if new_wines == 0:
                        break

                    if page % 5 == 0:  # Save every 5 pages
                        save_to_json(batch, batch_num)
                        batch = []
                        batch_num += 1

                    page += 1
                    time.sleep(delay)

                except Exception as e:
                    print(f"❌ Error on page {page}: {e}")
                    break

    if batch:
        save_to_json(batch, batch_num)

    return list(all_wines.values())

if __name__ == "__main__":
    wines = fetch_all_wines()
    print(f"\n✅ Completed scraping. Saved {len(wines)} wines to JSON files.")