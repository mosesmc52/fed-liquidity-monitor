import mimetypes
from email.message import EmailMessage
from email.utils import make_msgid
from pathlib import Path
from typing import Iterable, Sequence

import boto3


class AmazonSES(object):

    def __init__(self, region, access_key, secret_key, from_address, charset="UTF-8"):
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key

        self.client = boto3.client(
            "ses",
            region_name=self.region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
        self.CHARSET = charset
        self.from_address = from_address

    # ---------------- existing simple helpers ----------------
    def send_text_email(self, to_address, subject, content):
        self.client.send_email(
            Destination={"ToAddresses": [to_address]},
            Message={
                "Body": {"Text": {"Charset": self.CHARSET, "Data": content}},
                "Subject": {"Charset": self.CHARSET, "Data": subject},
            },
            Source=self.from_address,
        )

    def send_html_email(self, to_address, subject, content):
        self.client.send_email(
            Destination={"ToAddresses": [to_address]},
            Message={
                "Body": {"Html": {"Charset": self.CHARSET, "Data": content}},
                "Subject": {"Charset": self.CHARSET, "Data": subject},
            },
            Source=self.from_address,
        )

    # ---------------- NEW: HTML with inline CID images ----------------
    def send_html_email_with_inline_images(
        self,
        to_addresses: Sequence[str],
        subject: str,
        html_body: str,
        image_paths: Iterable[Path] | None = None,
    ):
        """
        Send an HTML email where images are displayed inline using CIDs.
        Gmail-compatible (multipart/related via SES SendRawEmail).

        Usage pattern:
          - In your HTML, put <img src="cid:filename.png"> where you want each image.
          - Pass the corresponding Path objects in `image_paths`. This method
            replaces each 'cid:{filename}' with a generated CID and attaches the image.

        :param to_addresses: list of recipients
        :param subject: email subject
        :param html_body: HTML string (can contain <img src="cid:filename"> placeholders)
        :param image_paths: iterable of Path objects for files you want inline
        """

        to_addresses = [a for a in to_addresses if a]  # sanitize
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_address
        msg["To"] = ", ".join(to_addresses)

        # Plain text fallback
        msg.set_content("Open this email in an HTML-capable client to view the charts.")

        # Prepare CID map (filename -> generated cid)
        cid_map: dict[Path, str] = {}
        if image_paths:
            for p in image_paths:
                p = Path(p)
                if not p.exists():
                    continue
                cid_map[p] = make_msgid(domain="local").strip("<>")

            # Replace cid:filename placeholders with cid:{actual_cid}
            for p, cid in cid_map.items():
                html_body = html_body.replace(f"cid:{p.name}", f"cid:{cid}")

        # Add HTML part
        msg.add_alternative(html_body, subtype="html")

        # Attach the related images to the HTML part
        if cid_map:
            html_part = msg.get_payload()[1]  # the text/html part
            for p, cid in cid_map.items():
                mtype, _ = mimetypes.guess_type(str(p))
                if not mtype:
                    maintype, subtype = "image", "png"
                else:
                    maintype, subtype = mtype.split("/", 1)
                html_part.add_related(
                    p.read_bytes(),
                    maintype=maintype,
                    subtype=subtype,
                    cid=f"<{cid}>",
                    filename=p.name,
                )

        # Send via SendRawEmail so multiparts + related are preserved

        self.client.send_raw_email(
            Source=self.from_address,
            Destinations=list(to_addresses),
            RawMessage={"Data": msg.as_bytes()},
        )
