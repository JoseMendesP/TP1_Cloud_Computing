import os
import time
import re
import logging
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from typing import Optional, List

import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import NoCredentialsError

@dataclass
class CreatureData:
    index: int
    name: str
    url: str
    types: List[str] = None
    image_url: Optional[str] = None

class WebScraper:
    def __init__(self, base_url: str, delay: float = 1.0):
        self.base_url = base_url
        self.delay = delay
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def fetch_page(self, url: str) -> BeautifulSoup:
        response = self.session.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

    def extract_creature_data(self, element) -> Optional[CreatureData]:
        cells = element.find_all("td")
        if len(cells) < 3:
            return None
        index_match = re.search(r"\d+", cells[0].get_text(strip=True))
        if not index_match:
            return None
        link_element = element.find("a", href=True)
        if not link_element:
            return None
        return CreatureData(
            index=int(index_match.group()),
            name=link_element.get_text(strip=True),
            url=urljoin(self.base_url, link_element["href"])
        )

class S3Uploader:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3")

    def upload_image(self, image_data: bytes, filename: str, prefix: str) -> Optional[str]:
        s3_key = f"{prefix}{filename}"
        try:
            ext = os.path.splitext(filename)[-1].lower()
            content_type = {
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".png": "image/png",
                ".gif": "image/gif"
            }.get(ext, "image/png")
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=image_data,
                ACL="public-read",
                ContentType=content_type
            )
            return f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"
        except NoCredentialsError:
            logging.error("AWS credentials not found!")
            return None
        except Exception as e:
            logging.error(f"Failed to upload {filename} to S3: {e}")
            return None

class DataCollector:
    def __init__(self, bucket_name: str):
        self.base_url = "https://bulbapedia.bulbagarden.net"
        self.list_url = f"{self.base_url}/wiki/List_of_Pok%C3%A9mon_by_National_Pok%C3%A9dex_number"
        self.scraper = WebScraper(self.base_url)
        self.uploader = S3Uploader(bucket_name)

    def find_creature_image_and_types(self, url: str) -> (Optional[str], List[str]):
        try:
            soup = self.scraper.fetch_page(url)
            info_table = soup.find("table", {"class": re.compile(r"infobox|roundy")})
            if not info_table:
                return None, []
            img_tag = info_table.find("img")
            image_url = None
            if img_tag and img_tag.get("src"):
                src = img_tag["src"]
                image_url = f"https:{src}" if src.startswith("//") else src

            type_cells = info_table.find_all("a", href=re.compile("/wiki/.*_type"))
            types = [t.get_text(strip=True).lower() for t in type_cells if t.get_text(strip=True)]
            return image_url, types or ["unknown"]
        except Exception:
            return None, ["unknown"]

    def collect_data(self, limit: int = 100):
        soup = self.scraper.fetch_page(self.list_url)
        tables = soup.find_all("table", {"class": re.compile(r"(roundy|sortable)")})
        count = 0
        for table in tables:
            for row in table.find_all("tr"):
                if count >= limit:
                    logging.info("Limite atteinte.")
                    return
                creature = self.scraper.extract_creature_data(row)
                if not creature:
                    continue
                logging.info(f"Processing #{creature.index:04d} {creature.name}")
                image_url, types = self.find_creature_image_and_types(creature.url)
                if not image_url:
                    logging.warning(f"No image found for {creature.name}")
                    continue
                prefix = f"images/{types[0]}/"
                ext = os.path.splitext(urlparse(image_url).path)[-1] or ".png"
                filename = f"{creature.index:04d}_{creature.name}{ext}"
                try:
                    headers = {"User-Agent": "Mozilla/5.0"}
                    image_data = requests.get(image_url, headers=headers).content
                    s3_url = self.uploader.upload_image(image_data, filename, prefix=prefix)
                    if s3_url:
                        logging.info(f"Uploaded to S3: {s3_url}")
                        count += 1
                except Exception as e:
                    logging.error(f"Failed to download {image_url}: {e}")
                time.sleep(self.scraper.delay)

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    bucket_name = "pokemon-scraper-binks"
    collector = DataCollector(bucket_name)
    collector.collect_data(limit=100)

if __name__ == "__main__":
    main()
