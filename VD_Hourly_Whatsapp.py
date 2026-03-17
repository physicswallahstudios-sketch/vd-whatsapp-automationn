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
import google.auth
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import re

SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "VD Top Batch Day View 16th Mar Onwards"

utc_now = datetime.now(pytz.utc)
event_start_date = datetime(2026, 3, 16, tzinfo=pytz.utc)
day_diff = (utc_now.date() - event_start_date.date()).days

DAY_RANGES = [
    [ # Day 0
        f"{SHEET_NAME}!A5:F20",
        f"{SHEET_NAME}!L6:Q20",
    ],
    [ # Day 1 - test
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

def parse_a1_notation(range_str: str):
    """Parses A1 notation like 'A5:F20' into grid indices."""
    parts = range_str.split("!")[-1].split(":")
    if len(parts) != 2:
        return None
    
    def decode_col(col_str):
        col = 0
        for char in col_str:
            col = col * 26 + (ord(char.upper()) - ord('A') + 1)
        return col - 1

    match_start = re.match(r"([A-Za-z]+)([0-9]+)", parts[0])
    match_end = re.match(r"([A-Za-z]+)([0-9]+)", parts[1])
    
    if not match_start or not match_end:
        return None
        
    start_col = decode_col(match_start.group(1))
    start_row = int(match_start.group(2)) - 1
    end_col = decode_col(match_end.group(1))
    end_row = int(match_end.group(2))
    
    return start_row, end_row, start_col, end_col

def get_sheet_layout(creds: Credentials, sheet_name: str):
    """Fetches row heights and column widths for the sheet."""
    try:
        service = build("sheets", "v4", credentials=creds)
        # Optimized: Only fetch metadata for the specific sheet
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=SHEET_ID, 
            fields="sheets(properties,data(columnMetadata,rowMetadata))"
        ).execute()
        
        for sheet in spreadsheet["sheets"]:
            if sheet["properties"]["title"] == sheet_name:
                data = sheet.get("data", [{}])[0]
                col_metadata = data.get("columnMetadata", [])
                row_metadata = data.get("rowMetadata", [])
                
                # Google Sheets default: 100 for column, 21 for row if not specified
                col_widths = [m.get("pixelSize", 100) for m in col_metadata]
                row_heights = [m.get("pixelSize", 21) for m in row_metadata]
                
                if not col_widths or not row_heights:
                    logger.warning("found sheet but metadata is empty")
                    return None, None
                    
                return col_widths, row_heights
    except Exception as e:
        logger.error("error fetching sheet layout: %s", str(e))
    return None, None

def export_bounding_box_as_pdf(creds: Credentials, sheet_gid: str, range_str: str) -> bytes:
    """Exports a specific bounding box as PDF with retries."""
    
    # Revert to the docs.google.com URL as it's more flexible with gid and range
    # but use it with a specific range to avoid 403s on "Full Sheet"
    export_url = (
        f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export"
        f"?format=pdf"
        f"&portrait=false"
        f"&gid={sheet_gid}"
        f"&range={range_str}"
        f"&size=A3"         
        f"&scale=2"       
        f"&top_margin=0.1"
        f"&bottom_margin=0.1"
        f"&left_margin=0.1"
        f"&right_margin=0.1"
        f"&fzr=false"
        f"&gridlines=false"
        f"&printtitle=false"
    )

    backoff = [2, 5, 10]
    for attempt, delay in enumerate(backoff + [0]):
        try:
            logger.info("exporting bounding box %s (attempt %d/%d)", range_str, attempt + 1, len(backoff)+1)
            response = requests.get(
                export_url,
                headers={"Authorization": f"Bearer {creds.token}"},
                timeout=120,
            )
            
            # Validation
            if response.status_code != 200:
                logger.error("export failed status=%s body=%s", response.status_code, response.text[:500])
            elif "application/pdf" not in response.headers.get("Content-Type", ""):
                logger.error("export returned non-PDF content-type=%s body=%s", 
                             response.headers.get("Content-Type"), response.text[:500])
            elif len(response.content) < 1024:
                logger.error("export response too small: %d bytes", len(response.content))
            else:
                logger.info("successfully exported PDF: %.2f KB", len(response.content) / 1024)
                return response.content

        except Exception as e:
            logger.error("export request error: %s", str(e))

        if delay:
            logger.info("retrying in %ds...", delay)
            time.sleep(delay)
            refresh_creds(creds)
            
    return None

def crop_range_from_image(img: Image.Image, range_str: str, bbox_indices: tuple, col_widths: List[int], row_heights: List[int], dpi: int = 300) -> Image.Image:
    """Crops a specific range from the bounding box image."""
    target_indices = parse_a1_notation(range_str)
    if not target_indices:
        logger.warning("could not parse target range %s", range_str)
        return img
    
    # Bounding Box Offset
    bb_start_row, _, bb_start_col, _ = bbox_indices
    start_row, end_row, start_col, end_col = target_indices
    
    # Scale factor (Sheets metadata is at 96 DPI, output is at specified DPI)
    # The export scale (e.g. scale=2) is handled by the PDF generator, 
    # but the metadata is in 'points' (96 per inch).
    # We need to find the pixel ratio based on the final image size vs metadata calculation.
    
    # Total width/height of the bounding box in points
    bb_width_pts = sum(col_widths[bb_start_col:indices_max_col(bbox_indices)])
    bb_height_pts = sum(row_heights[bb_start_row:indices_max_row(bbox_indices)])
    
    img_w, img_h = img.size
    scale_w = img_w / float(bb_width_pts)
    scale_h = img_h / float(bb_height_pts)
    
    # Calculate pixel bounds relative to the crop
    # relative_start_col = target_start_col - bb_start_col
    left = sum(col_widths[bb_start_col:start_col]) * scale_w
    top = sum(row_heights[bb_start_row:start_row]) * scale_h
    right = left + sum(col_widths[start_col:end_col]) * scale_w
    bottom = top + sum(row_heights[start_row:end_row]) * scale_h
    
    # Safety
    left = max(0, min(left, img_w))
    top = max(0, min(top, img_h))
    right = max(left + 1, min(right, img_w))
    bottom = max(top + 1, min(bottom, img_h))
    
    logger.info("cropping %s from bounding box at relative (%.1f, %.1f, %.1f, %.1f)", 
                range_str, left, top, right, bottom)
    return img.crop((int(left), int(top), int(right), int(bottom)))

def indices_max_col(indices): return indices[3]
def indices_max_row(indices): return indices[1]

def calculate_daily_bounding_box(ranges: List[str]) -> (str, tuple):
    """Finds the minimal range string that covers all target ranges."""
    parsed = [parse_a1_notation(r) for r in ranges]
    parsed = [p for p in parsed if p]
    if not parsed:
        return None, None
    
    min_row = min(p[0] for p in parsed)
    max_row = max(p[1] for p in parsed)
    min_col = min(p[2] for p in parsed)
    max_col = max(p[3] for p in parsed)
    
    def encode_col(n):
        res = ""
        while n >= 0:
            res = chr(n % 26 + ord('A')) + res
            n = n // 26 - 1
        return res

    bbox_str = f"{encode_col(min_col)}{min_row+1}:{encode_col(max_col-1)}{max_row}"
    return bbox_str, (min_row, max_row, min_col, max_col)

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
    
    # 0. Calculate Bounding Box
    bbox_str, bbox_indices = calculate_daily_bounding_box(RANGES)
    if not bbox_str:
        logger.error("failed to calculate bounding box for ranges")
        return []

    # 1. Fetch layout metadata
    logger.info("fetching sheet layout metadata...")
    col_widths, row_heights = get_sheet_layout(creds, SHEET_NAME)
    if not col_widths or not row_heights:
        logger.error("failed to fetch sheet layout, cannot proceed with robust cropping")
        return []

    # 2. Export Bounding Box as PDF
    pdf_content = export_bounding_box_as_pdf(creds, sheet_gid, bbox_str)
    if not pdf_content:
        logger.error("failed to export PDF after all retries")
        return []

    # 3. Convert PDF to image
    logger.info("converting PDF to image...")
    try:
        pages = convert_from_bytes(pdf_content, dpi=300)
        if not pages:
            logger.error("pdf2image returned no pages")
            return []
        # In case the bounding box spans multiple pages (unlikely with A3), merge them or check
        full_img = pages[0].convert("RGB")
    except Exception as e:
        logger.critical("pdf2image CRASHED: %s", str(e), exc_info=True)
        return []

    uploaded_urls = []

    # 4. Process each range from the bounding box image
    for i, sheet_range in enumerate(RANGES, start=1):
        try:
            logger.info("processing range %d/%d: %s", i, len(RANGES), sheet_range)
            
            # Crop range relative to bounding box
            img = crop_range_from_image(full_img, sheet_range, bbox_indices, col_widths, row_heights, dpi=300)
            
            # Polish and optimize
            img = ImageEnhance.Sharpness(img).enhance(1.2)
            img = crop_white_space(img) 
            
            jpg_data = optimize_image(img)

            filename = f"table_{i}.jpg"
            with open(filename, "wb") as f:
                f.write(jpg_data)

            try:
                with open(filename, "rb") as f:
                    upload = requests.post(
                        UPLOAD_URL,
                        files={"file": f},
                        data={
                            "upload_preset": UPLOAD_PRESET,
                            "folder": f"BizCat_Exports/{datetime.now(pytz.utc).strftime('%Y-%m-%d')}",
                        },
                        timeout=90,
                    )
                    if upload.status_code != 200:
                        logger.error("Cloudinary upload failed for range %s: %s", sheet_range, upload.text)
                        continue
                        
                url = upload.json().get("secure_url")
                if url:
                    uploaded_urls.append(url)
                    logger.info("uploaded %s", url)
            except Exception as e:
                logger.error("error during upload of range %s: %s", sheet_range, str(e))
            finally:
                if os.path.exists(filename):
                    os.remove(filename)
        except Exception as e:
            logger.error("failed to process range %s: %s", sheet_range, str(e), exc_info=True)

        time.sleep(1)

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

            try:
                r = requests.post(
                    "https://backend.aisensy.com/campaign/t1/api",
                    json=payload,
                    timeout=30,
                )
                if r.status_code == 200:
                    logger.info("sent to %s image %s status 200", dest, i)
                else:
                    logger.error("failed to send to %s image %s status %d body %s", 
                                 dest, i, r.status_code, r.text)
            except Exception as e:
                logger.error("error sending to aisensy: %s", str(e))
            
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

    try:
        logger.info("automation started for Day %s (UTC date: %s)", max_day_index, datetime.now(pytz.utc).date())
        urls = export_and_upload_images()
        send_via_aisensy(urls)
        logger.info("automation completed")
    except Exception as e:
        logger.critical("FATAL ERROR in main loop: %s", str(e), exc_info=True)
        # We catch everything to prevent container crash, but exit normally
