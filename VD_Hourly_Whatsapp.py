#!/usr/bin/env python3

import os
import time
import io
import logging
import tempfile
import pytz
from datetime import datetime
from typing import List
import json

import requests
from PIL import Image, ImageEnhance, ImageChops
from pdf2image import convert_from_bytes
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "VD Top Batch Day View 16th Mar Onwards"

utc_now = datetime.now(pytz.utc)
event_start_date = datetime(2026, 2, 28, tzinfo=pytz.utc)
day_diff = (utc_now.date() - event_start_date.date()).days

DAY_RANGES = [
    [ # Day 0
        f"{SHEET_NAME}!A5:F20",
        f"{SHEET_NAME}!L6:Q20",
    ],
    [ # Day 1
        f"{SHEET_NAME}!A21:F37",
        f"{SHEET_NAME}!L23:Q37",
    ],
    [ # Day 2
        f"{SHEET_NAME}!A38:F54",
        f"{SHEET_NAME}!L40:Q54",
    ],
    [ # Day 3
        f"{SHEET_NAME}!A55:F71",
        f"{SHEET_NAME}!L57:Q71",
    ],
    [ # Day 4
        f"{SHEET_NAME}!A72:F88",
        f"{SHEET_NAME}!L74:Q88",
    ],
    [ # Day 5
        f"{SHEET_NAME}!A89:F105",
        f"{SHEET_NAME}!L91:Q105",
    ],
    [ # Day 6
        f"{SHEET_NAME}!A106:F122",
        f"{SHEET_NAME}!L108:Q122",
    ],
    [ # Day 7
        f"{SHEET_NAME}!A123:F139",
        f"{SHEET_NAME}!L125:Q139",
    ],
    [ # Day 8
        f"{SHEET_NAME}!A140:F156",
        f"{SHEET_NAME}!L142:Q156",
    ],
    [ # Day 9
        f"{SHEET_NAME}!A157:F173",
        f"{SHEET_NAME}!L159:Q173",
    ],
    [ # Day 10
        f"{SHEET_NAME}!A174:F190",
        f"{SHEET_NAME}!L176:Q190",
    ],
    [ # Day 11
        f"{SHEET_NAME}!A191:F207",
        f"{SHEET_NAME}!L193:Q207",
    ],
    [ # Day 12
        f"{SHEET_NAME}!A208:F224",
        f"{SHEET_NAME}!L210:Q224",
    ],
    [ # Day 13
        f"{SHEET_NAME}!A225:F241",
        f"{SHEET_NAME}!L227:Q241",
    ],
    [ # Day 14
        f"{SHEET_NAME}!A242:F258",
        f"{SHEET_NAME}!L244:Q258",
    ],
    [ # Day 15
        f"{SHEET_NAME}!A259:F275",
        f"{SHEET_NAME}!L261:Q275",
    ]
]

max_day_index = min(max(0, day_diff), 15)
RANGES = DAY_RANGES[max_day_index]

CLOUD_NAME = os.getenv("CLOUD_NAME")
UPLOAD_PRESET = os.getenv("UPLOAD_PRESET")
UPLOAD_URL = f"https://api.cloudinary.com/v1_1/{CLOUD_NAME}/image/upload"

AISENSY_API_KEY = os.getenv("AISENSY_API_KEY")
CAMPAIGN_NAME = os.getenv("AISENSY_CAMPAIGN_NAME")
DESTINATIONS = [d.strip() for d in os.getenv("DESTINATIONS", "").split(",") if d.strip()]

TODAY = utc_now.strftime("%d %B %Y")

TARGET_SIZE_BYTES = 4 * 1024 * 1024
JPEG_QUALITIES = [95, 85, 75, 65, 55]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("bizcat")

def refresh_creds(creds: Credentials):
    if not creds.valid:
        creds.refresh(Request())
        logger.info("google token refreshed")

def get_sheet_gid(creds: Credentials, sheet_name: str) -> str:
    service = build("sheets", "v4", credentials=creds)
    meta = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for sheet in meta["sheets"]:
        props = sheet["properties"]
        if props["title"] == sheet_name:
            return str(props["sheetId"])
    raise RuntimeError(f"sheet {sheet_name} not found")

def jpg_bytes(img: Image.Image, quality: int) -> bytes:
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True, progressive=True)
    return buf.getvalue()

def optimize_image(img: Image.Image) -> bytes:
    if img.mode != "RGB":
        img = img.convert("RGB")

    for q in JPEG_QUALITIES:
        data = jpg_bytes(img, q)
        logger.info("jpeg quality %s size %.2f MB", q, len(data) / 1024 / 1024)
        if len(data) <= TARGET_SIZE_BYTES:
            return data

    w, h = img.size
    for _ in range(3):
        w = int(w * 0.96)
        h = int(h * 0.96)
        img = img.resize((w, h), Image.LANCZOS)
        data = jpg_bytes(img, 65)
        if len(data) <= TARGET_SIZE_BYTES:
            return data

    return data

def crop_white_space(img: Image.Image) -> Image.Image:
    bg = Image.new(img.mode, img.size, img.getpixel((0, 0)))
    diff = ImageChops.difference(img, bg)
    diff = ImageEnhance.Contrast(diff).enhance(3.0)
    bbox = diff.getbbox()
    return img.crop(bbox) if bbox else img

def export_and_upload_images() -> List[str]:
    creds_info = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))

    creds = Credentials.from_service_account_info(
        creds_info,
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets.readonly",
        ],
    )

    refresh_creds(creds)
    sheet_gid = get_sheet_gid(creds, SHEET_NAME)
    logger.info("using sheet %s gid=%s", SHEET_NAME, sheet_gid)

    uploaded_urls = []

    for i, sheet_range in enumerate(RANGES, start=1):
        range_only = sheet_range.split("!")[1]

        export_url = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
            f"?format=pdf"
            f"&portrait=false"
            f"&gid={sheet_gid}"
            f"&range={range_only}"
            f"&size=A2"
            f"&scale=5"
            f"&top_margin=0.25"
            f"&bottom_margin=0.25"
            f"&left_margin=0.25"
            f"&right_margin=0.25"
            f"&fzr=false"
            f"&gridlines=false"
            f"&printtitle=false"
        )

        logger.info("exporting range %s", sheet_range)

        response = requests.get(
            export_url,
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=90,
        )
        response.raise_for_status()

        pages = convert_from_bytes(
            response.content,
            dpi=300,
            first_page=1,
            last_page=1,
        )

        img = pages[0].convert("RGB")
        img = ImageEnhance.Sharpness(img).enhance(2.0)
        img = crop_white_space(img)

        jpg_data = optimize_image(img)

        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_table_{i}.jpg") as tmp:
            tmp.write(jpg_data)
            filename = tmp.name

        try:
            with open(filename, "rb") as f:
                upload = requests.post(
                    UPLOAD_URL,
                    files={"file": f},
                    data={
                        "upload_preset": UPLOAD_PRESET,
                        "folder": f"BizCat_Exports/{datetime.now(pytz.utc).strftime('%Y-%m-%d')}",
                    },
                    timeout=60,
                )
                upload.raise_for_status()

            url = upload.json().get("secure_url")
            if url:
                uploaded_urls.append(url)
                logger.info("uploaded %s", url)
        finally:
            os.remove(filename)

        time.sleep(2)

    return uploaded_urls

def send_via_aisensy(urls: List[str]):
    if not urls:
        logger.warning("no images generated")
        return

    for dest in DESTINATIONS:
        for i, url in enumerate(urls, start=1):
            payload = {
                "apiKey": AISENSY_API_KEY,
                "campaignName": CAMPAIGN_NAME,
                "destination": dest,
                "userName": "PW Online- Analytics",
                "templateParams": [TODAY],
                "source": "automation-script",
                "media": {"url": url, "filename": f"table_{i}.jpg"},
            }

            r = requests.post(
                "https://backend.aisensy.com/campaign/t1/api",
                json=payload,
                timeout=30,
            )
            logger.info("sent to %s image %s status %s", dest, i, r.status_code)
            time.sleep(5)

if __name__ == "__main__":
    required = [
        "SHEET_ID",
        "CLOUD_NAME",
        "UPLOAD_PRESET",
        "AISENSY_API_KEY",
        "DESTINATIONS",
    ]

    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise EnvironmentError(f"missing secrets: {', '.join(missing)}")


    Image.MAX_IMAGE_PIXELS = 300_000_000

    logger.info("automation started for Day %s (UTC date: %s)", max_day_index, datetime.now(pytz.utc).date())
    urls = export_and_upload_images()
    send_via_aisensy(urls)
    logger.info("automation completed successfully")
