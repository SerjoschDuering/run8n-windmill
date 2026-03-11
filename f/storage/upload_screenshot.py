"""
Upload screenshot to S3 with automatic TTL/expiry handling.

Uploads base64-encoded PNG to run8n-public bucket with a unique key.
Files are organized under screenshots/{date}/{uuid}.png
"""

import base64
import uuid
from datetime import datetime
from typing import Optional

import boto3
import wmill
from botocore.config import Config


S3_ENDPOINT = "https://s3.eu-central-003.backblazeb2.com"
S3_REGION = "eu-central-003"
BUCKET = "run8n-public"
PUBLIC_URL_BASE = "https://f003.backblazeb2.com/file/run8n-public"


def main(
    screenshot_base64: str,
    prefix: str = "screenshots",
    filename: Optional[str] = None,
    content_type: str = "image/png",
    s3_key: Optional[str] = None,
    s3_secret: Optional[str] = None,
) -> dict:
    """
    Upload screenshot to S3 and return public URL.

    Args:
        screenshot_base64: Base64-encoded image data (with or without data: prefix)
        prefix: S3 key prefix (default: screenshots)
        filename: Optional custom filename (default: auto-generated UUID)
        content_type: MIME type (default: image/png)
        s3_key: S3 access key (uses S3_PUBLIC_KEY from Windmill resources if not provided)
        s3_secret: S3 secret (uses S3_PUBLIC_SECRET from Windmill resources if not provided)

    Returns:
        {
            "url": "https://f003.backblazeb2.com/file/run8n-public/screenshots/2024-01-28/abc123.png",
            "key": "screenshots/2024-01-28/abc123.png",
            "bucket": "run8n-public",
            "size_bytes": 12345,
            "status": "ok"
        }

    Note: Backblaze B2 doesn't support native TTL/lifecycle rules on free tier.
    Consider running a cleanup cron job to delete old screenshots.
    """
    try:
        # Strip data URL prefix if present
        if screenshot_base64.startswith("data:"):
            screenshot_base64 = screenshot_base64.split(",", 1)[1]

        # Decode base64
        image_data = base64.b64decode(screenshot_base64)
        size_bytes = len(image_data)

        # Generate key (sanitize prefix and filename to prevent path traversal)
        import re
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        safe_prefix = re.sub(r'[^a-zA-Z0-9_\-/]', '', prefix).strip('/')
        raw_file_id = filename or f"{uuid.uuid4().hex[:12]}.png"
        safe_file_id = re.sub(r'[^a-zA-Z0-9_\-.]', '', raw_file_id)
        s3_path = f"{safe_prefix}/{date_str}/{safe_file_id}"

        # Get credentials from Windmill resources if not provided
        if not s3_key or not s3_secret:
            try:
                resources = wmill.get_resource("f/shared/s3_public")
                s3_key = s3_key or resources.get("access_key")
                s3_secret = s3_secret or resources.get("secret_key")
            except Exception:
                return {
                    "status": "error",
                    "error": "S3 credentials not provided and Windmill resource not found",
                }

        # Create S3 client
        client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            region_name=S3_REGION,
            aws_access_key_id=s3_key,
            aws_secret_access_key=s3_secret,
            config=Config(signature_version="s3v4"),
        )

        # Upload
        client.put_object(
            Bucket=BUCKET,
            Key=s3_path,
            Body=image_data,
            ContentType=content_type,
            # Add metadata for tracking
            Metadata={
                "uploaded-at": datetime.utcnow().isoformat(),
                "source": "env-analysis-mcp",
            },
        )

        public_url = f"{PUBLIC_URL_BASE}/{s3_path}"

        return {
            "url": public_url,
            "key": s3_path,
            "bucket": BUCKET,
            "size_bytes": size_bytes,
            "status": "ok",
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }
