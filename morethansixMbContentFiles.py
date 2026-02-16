import os
import mimetypes
import requests
import json
import hashlib
from datetime import datetime
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from pymongo import MongoClient

# ================= ENV =================
load_dotenv()

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN    = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN   = os.getenv("SF_DOMAIN", "test")

DMS_URL     = os.getenv("DMS_ENDPOINT")
DMS_AUTH    = os.getenv("DMS_AUTH_HEADER")

MONGO_URI   = os.getenv("MONGO_URI")

DOWNLOAD_DIR = "./downloads"
MAX_SIZE = 6 * 1024 * 1024  # 6 MB


# ================= SALESFORCE LOGIN =================

def sf_login():
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_TOKEN,
        domain=SF_DOMAIN
    )
    print("LOGIN OK ✅", sf.sf_instance)
    return sf


# ================= SHA1 CHECKSUM =================

def generate_sha1(file_bytes):
    sha1 = hashlib.sha1()
    sha1.update(file_bytes)
    return sha1.hexdigest()


# ================= MONGO SETUP =================

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["dms_tracking"]
collection = db["uploaded_files"]


# ================= MAIN PROCESS =================

def main():

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    sf = sf_login()

    start = input("Start Date (YYYY-MM-DD): ")
    end   = input("End Date (YYYY-MM-DD): ")

    query = f"""
    SELECT Id, Title, FileExtension, ContentSize, CreatedDate, CreatedBy.Name
    FROM ContentVersion
    WHERE CreatedDate >= {start}T00:00:00Z
    AND CreatedDate <= {end}T23:59:59Z
    AND ContentSize > {MAX_SIZE}
    """

    records = sf.query_all(query)["records"]
    print(f"\nFound {len(records)} eligible files\n")

    for r in records:

        try:
            cv_id = r["Id"]
            title = r["Title"]
            ext = r["FileExtension"]

            # Avoid duplicate extension
            if not title.lower().endswith(f".{ext.lower()}"):
                filename = f"{title}.{ext}"
            else:
                filename = title

            file_extension = ext.lower()

            # ================= DOWNLOAD FILE =================
            download_url = f"{sf.base_url}sobjects/ContentVersion/{cv_id}/VersionData"

            sf_resp = requests.get(
                download_url,
                headers={"Authorization": f"Bearer {sf.session_id}"},
                timeout=300
            )
            sf_resp.raise_for_status()

            file_bytes = sf_resp.content

            print(f"Downloaded → {filename} ({len(file_bytes)} bytes)")

            # ================= FORMAT DATE (DD/MM/YYYY) =================
            sf_date = r["CreatedDate"]
            parsed_date = datetime.strptime(sf_date[:10], "%Y-%m-%d")
            formatted_date = parsed_date.strftime("%d/%m/%Y")

            # ================= CHECKSUM =================
            checksum = generate_sha1(file_bytes)

            # ================= BUILD METADATA =================
            metadata = {
                "vertical": "VF_Gallop",
                "user": r["CreatedBy"]["Name"],
                "uniqueId": cv_id,
                "status": "1",
                "stage": "FI",
                "sourceSys": "Gallop",
                "size": str(len(file_bytes)),
                "module": "Notice Letters",
                "latLong": "242-12344",
                "keyValue": [title],
                "keyId": ["agreementNo"],
                "imageSubCategory": "0",
                "imageSrcId": "01",
                "imageSrc": "Gallop",
                "imageId": cv_id,
                "imageCategory": "0",
                "format": file_extension,
                "fileName": filename,
                "createdDate": formatted_date,   # ✅ Correct format
                "createdBy": r["CreatedBy"]["Name"],
                "checkSum": checksum,
                "branchCode": "1208",
                "branch": "HO",
                "appId": None
            }

            metadata_json = json.dumps(metadata)

            # ================= MULTIPART UPLOAD =================
            files = {
                "data": (None, metadata_json, "application/json"),
                "image": (
                    filename,
                    file_bytes,
                    mimetypes.guess_type(filename)[0] or "application/octet-stream"
                )
            }

            headers = {
                "Authorization": DMS_AUTH
            }

            print(f"Uploading → {filename}")

            dms_resp = requests.post(
                DMS_URL,
                headers=headers,
                files=files,
                timeout=300
            )

            print("STATUS:", dms_resp.status_code)
            print("BODY:", dms_resp.text)

            # ================= RESULT TRACKING =================
            if dms_resp.status_code in [200, 201]:

                collection.insert_one({
                    "contentDocumentId": cv_id,
                    "fileName": filename,
                    "status": "Success",
                    "dmsResponse": dms_resp.text
                })

            else:

                collection.insert_one({
                    "contentDocumentId": cv_id,
                    "fileName": filename,
                    "status": "Failed",
                    "dmsResponse": dms_resp.text
                })

            print("-" * 60)

        except Exception as e:

            print("ERROR:", str(e))

            collection.insert_one({
                "contentDocumentId": r.get("Id"),
                "fileName": r.get("Title"),
                "status": "Exception",
                "dmsResponse": str(e)
            })

    print("\n✔ Processing Completed")


if __name__ == "__main__":
    main()
