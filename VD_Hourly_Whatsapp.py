#!/usr/bin/env python3

import os
import time
import io
import logging
import tempfile
import pytz
from datetime import datetime, timedelta
from typing import List
import json

import requests
from PIL import Image, ImageEnhance, ImageChops
from pdf2image import convert_from_bytes
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build


# =========================
# ENV VARIABLES & SETTINGS
# =========================
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "VD Top Batch Day View 1st April Onwards"
IST = pytz.timezone("Asia/Kolkata")
EVENT_START_DATE = datetime(2026, 5, 1, tzinfo=IST).date()

# =========================
# SHEET RANGES
# =========================
DAY_RANGES = [
    [f"{SHEET_NAME}!A590:F610", f"{SHEET_NAME}!K592:R610"],     # Day 0
    [f"{SHEET_NAME}!A611:F631", f"{SHEET_NAME}!K613:R631"],     # Day 1
    [f"{SHEET_NAME}!A632:F652", f"{SHEET_NAME}!K634:R652"],     # Day 2
    [f"{SHEET_NAME}!A653:F673", f"{SHEET_NAME}!K655:R673"],     # Day 3
    [f"{SHEET_NAME}!A674:F694", f"{SHEET_NAME}!K676:R694"],     # Day 4
    [f"{SHEET_NAME}!A695:F715", f"{SHEET_NAME}!K697:R715"],     # Day 5
    [f"{SHEET_NAME}!A716:F736", f"{SHEET_NAME}!K718:R736"],     # Day 6
    [f"{SHEET_NAME}!A737:F757", f"{SHEET_NAME}!K739:R757"],     # Day 7
    [f"{SHEET_NAME}!A758:F778", f"{SHEET_NAME}!K760:R778"],     # Day 8
    [f"{SHEET_NAME}!A779:F799", f"{SHEET_NAME}!K781:R799"],     # Day 9
    [f"{SHEET_NAME}!A800:F820", f"{SHEET_NAME}!K802:R820"],     # Day 10
    [f"{SHEET_NAME}!A821:F841", f"{SHEET_NAME}!K823:R841"],     # Day 11
    [f"{SHEET_NAME}!A842:F862", f"{SHEET_NAME}!K844:R862"],     # Day 12
    [f"{SHEET_NAME}!A863:F883", f"{SHEET_NAME}!K865:R883"],     # Day 13
    [f"{SHEET_NAME}!A884:F904", f"{SHEET_NAME}!K886:R904"],     # Day 14
    [f"{SHEET_NAME}!A905:F925", f"{SHEET_NAME}!K907:R925"],     # Day 15
    [f"{SHEET_NAME}!A926:F946", f"{SHEET_NAME}!K928:R946"],     # Day 16
    [f"{SHEET_NAME}!A947:F967", f"{SHEET_NAME}!K949:R967"],     # Day 17
    [f"{SHEET_NAME}!A968:F988", f"{SHEET_NAME}!K970:R988"],     # Day 18
    [f"{SHEET_NAME}!A989:F1009", f"{SHEET_NAME}!K991:R1009"],   # Day 19
    [f"{SHEET_NAME}!A1010:F1030", f"{SHEET_NAME}!K1012:R1030"], # Day 20
    [f"{SHEET_NAME}!A1031:F1051", f"{SHEET_NAME}!K1033:R1051"], # Day 21
    [f"{SHEET_NAME}!A1052:F1072", f"{SHEET_NAME}!K1054:R1072"], # Day 22
    [f"{SHEET_NAME}!A1073:F1093", f"{SHEET_NAME}!K1075:R1093"], # Day 23
    [f"{SHEET_NAME}!A1100:F1123", f"{SHEET_NAME}!K1102:R1123"], # Day 24
    [f"{SHEET_NAME}!A1124:F1147", f"{SHEET_NAME}!K1126:R1147"], # Day 25
    [f"{SHEET_NAME}!A1148:F1171", f"{SHEET_NAME}!K1150:R1171"], # Day 26
    [f"{SHEET_NAME}!A1172:F1195", f"{SHEET_NAME}!K1174:R1195"], # Day 27
    [f"{SHEET_NAME}!A1178:F1198", f"{SHEET_NAME}!K1180:R1198"], # Day 28
    [f"{SHEET_NAME}!A1199:F1219", f"{SHEET_NAME}!K1201:R1219"], # Day 29
    [f"{SHEET_NAME}!A1220:F1240", f"{SHEET_NAME}!K1222:R1240"], # Day 30
]


def get_current_ranges():
    now_ist = datetime.now(IST)
    
    # Rollover logic: Before 5:00 AM IST, still consider it the previous reporting day
    cutoff_today = now_ist.replace(hour=5, minute=0, second=0, microsecond=0)
    if now_ist < cutoff_today:
        effective_date = (now_ist - timedelta(days=1)).date()
    else:
        effective_date = now_ist.date()
        
    day_diff = (effective_date - EVENT_START_DATE).days
    # The list has 31 items (index 0 to 30)
    day_index = min(max(0, day_diff), 30)
    
    logger.info("Reporting Day Index: %s (Effective Date: %s)", day_index, effective_date)
    return DAY_RANGES[day_index]


# =========================
# CLOUDINARY
# =========================
CLOUD_NAME = os.getenv("CLOUD_NAME")
UPLOAD_PRESET = os.getenv("UPLOAD_PRESET")
UPLOAD_URL = f"https://api.cloudinary.com/v1_1/{CLOUD_NAME}/image/upload"

# =========================
# AISENSY
# =========================
AISENSY_API_KEY = os.getenv("AISENSY_API_KEY")
CAMPAIGN_NAME = os.getenv("AISENSY_CAMPAIGN_NAME")
DESTINATIONS = [
    d.strip() for d in os.getenv("DESTINATIONS", "").split(",") if d.strip()
]

# =========================
# IMAGE SETTINGS
# =========================
TARGET_SIZE_BYTES = 4 * 1024 * 1024
JPEG_QUALITIES = [95, 85, 75, 65, 55]

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("bizcat")


# =========================
# HELPERS
# =========================
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

    # Try quality reduction
    for q in JPEG_QUALITIES:
        data = jpg_bytes(img, q)
        logger.info("jpeg quality %s size %.2f MB", q, len(data) / 1024 / 1024)
        if len(data) <= TARGET_SIZE_BYTES:
            return data

    # Resize fallback
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


# =========================
# MAIN LOGIC
# =========================
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
    
    now_ist = datetime.now(IST)
    now_mins = now_ist.hour * 60 + now_ist.minute
    
    # Execution Window: 8:30 AM to 1:30 AM (t+1)
    # If between 1:31 AM and 8:29 AM, skip.
    if 1 * 60 + 30 < now_mins < 8 * 60 + 30:
        logger.info("Current time %s is outside the scheduled window (08:30 - 01:30). Skipping.", now_ist.strftime("%H:%M"))
        return []

    # Get current day ranges
    day_ranges = get_current_ranges()
    sheet_gid = get_sheet_gid(creds, SHEET_NAME)
    
    tasks = []
    for r in day_ranges:
        tasks.append((SHEET_NAME, sheet_gid, r))

    uploaded_urls = []

    for i, (s_name, gid, s_range) in enumerate(tasks, start=1):
        range_only = s_range.split("!")[1]

        export_url = (
            f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
            f"?format=pdf&portrait=false&gid={gid}&range={range_only}"
            f"&size=A2&scale=5&top_margin=0.25&bottom_margin=0.25"
            f"&left_margin=0.25&right_margin=0.25&fzr=false"
            f"&gridlines=false&printtitle=false"
        )

        logger.info("Exporting task %s: %s (%s)", i, s_range, s_name)

        response = requests.get(
            export_url,
            headers={"Authorization": f"Bearer {creds.token}"},
            timeout=90,
        )
        response.raise_for_status()

        pages = convert_from_bytes(response.content, dpi=300, first_page=1, last_page=1)
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
                        "folder": f"BizCat_Exports/{now_ist.strftime('%Y-%m-%d')}",
                    },
                    timeout=60,
                )
                upload.raise_for_status()

            url = upload.json().get("secure_url")
            if url:
                uploaded_urls.append(url)
                logger.info("Uploaded %s: %s", s_range, url)

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
                "templateParams": [datetime.now(IST).strftime("%d %B %Y")],
                "source": "automation-script",
                "media": {
                    "url": url,
                    "filename": f"table_{i}.jpg"
                },
            }

            r = requests.post(
                "https://backend.aisensy.com/campaign/t1/api",
                json=payload,
                timeout=30,
            )

            logger.info("sent to %s image %s status %s", dest, i, r.status_code)
            time.sleep(5)


# =========================
# ENTRY POINT
# =========================
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

    logger.info("Automation run started (IST time: %s)", datetime.now(IST).strftime("%Y-%m-%d %H:%M"))
    
    try:
        urls = export_and_upload_images()
        if urls:
            send_via_aisensy(urls)
            logger.info("Automation run completed successfully")
        else:
            logger.info("No images to send. Automation run finished.")
    except Exception as e:
        logger.error("Error during automation run: %s", e, exc_info=True)
        import sys
        sys.exit(1)
