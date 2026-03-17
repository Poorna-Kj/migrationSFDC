import os
import json
import hashlib
import mimetypes
import requests
from datetime import datetime, timezone

from dotenv import load_dotenv
from simple_salesforce import Salesforce
from pymongo import MongoClient, ASCENDING

# ---------------- LOAD ENV ----------------

load_dotenv()

# ---------------- CONFIG ----------------

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN")

DMS_URL = os.getenv("DMS_ENDPOINT")
DMS_AUTH = os.getenv("DMS_AUTH_HEADER")

MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")

DOWNLOAD_DIR = "large_files"
MAX_SIZE = 6 * 1024 * 1024  # 6 MB

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# ---------------- DMS WRAPPER ----------------

class DMSRequestWrapper:

    @staticmethod
    def create_upload_request(r, doc_id, size, checksum, vertical, agreement_no, file_ext, filename):

        created_date = datetime.strptime(
            r["CreatedDate"][:10], "%Y-%m-%d"
        ).strftime("%d/%m/%Y")

        return {
            "imageId": doc_id,
            "uniqueId": r["Id"],
            "sourceSys": "Gallop",

            "branchCode": "1208",
            "branch": "HO",

            "vertical": vertical if vertical else "Unknown",

            "stage": "FI",
            "module": "Notice Letters",

            "imageCategory": "0",
            "imageSubCategory": "0",

            "status": "1",

            "fileName": filename,

            "latLong": "242-12344",
            "imageSrc": "Gallop",
            "imageSrcId": "01",

            "format": (file_ext or "").lower(),

            "user": r["CreatedBy"]["Name"],
            "size": str(size),

            "createdBy": r["CreatedBy"]["Name"],
            "createdDate": created_date,

            "checkSum": checksum,

            "keyId": ["agreementNo"],
            "keyValue": [agreement_no or "UNKNOWN"]
        }

# ---------------- LOGIN ----------------

def sf_login():
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_TOKEN,
        domain=SF_DOMAIN
    )
    print("✅ Salesforce Login Successful:", sf.sf_instance)
    return sf

# ---------------- MONGO ----------------

def mongo_connect():
    client = MongoClient(MONGO_URI)
    db = client["DMS_TRACKING_DB"]
    collection = db["DMS_FILE_METADATA"]

    collection.create_index(
        [("ContentDocumentId__c", ASCENDING)],
        unique=True
    )

    print("✅ MongoDB Connected")
    return collection

# ---------------- SHA1 ----------------

def generate_sha1(file_path):

    sha1 = hashlib.sha1()

    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha1.update(chunk)

    return sha1.hexdigest()

# ---------------- HELPERS ----------------

def build_filename(title, ext):
    if not ext:
        return title
    if title.lower().endswith(f".{ext.lower()}"):
        return title
    return f"{title}.{ext}"

# ---------------- SALESFORCE HELPERS ----------------

def get_linked_entity(sf, doc_id):

    query = f"""
    SELECT LinkedEntityId, LinkedEntity.Type
    FROM ContentDocumentLink
    WHERE ContentDocumentId = '{doc_id}'
    LIMIT 1
    """

    res = sf.query(query)

    if res["records"]:
        rec = res["records"][0]
        return rec["LinkedEntityId"], rec["LinkedEntity"]["Type"]

    return None, None


def get_vertical(sf, record_id, obj):

    if not record_id or not obj:
        return None

    try:
        query = f"""
        SELECT Vertical__c
        FROM {obj}
        WHERE Id = '{record_id}'
        LIMIT 1
        """
        res = sf.query(query)

        if res["records"]:
            return res["records"][0].get("Vertical__c")

    except Exception as e:
        print("❌ Vertical fetch failed:", str(e))

    return None


def get_agreement_number(sf, record_id, obj):

    if not record_id or not obj:
        return "UNKNOWN"

    try:
        query = f"""
        SELECT Agreement_No__c
        FROM {obj}
        WHERE Id = '{record_id}'
        LIMIT 1
        """
        res = sf.query(query)

        if res["records"]:
            return res["records"][0].get("Agreement_No__c") or "UNKNOWN"

    except Exception as e:
        print("❌ Agreement fetch failed:", str(e))

    return "UNKNOWN"

# ---------------- DOWNLOAD ----------------

def download_file(sf, cv_id, filename):

    url = f"{sf.base_url}sobjects/ContentVersion/{cv_id}/VersionData"

    headers = {
        "Authorization": f"Bearer {sf.session_id}"
    }

    path = os.path.join(DOWNLOAD_DIR, filename)

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

    print("📥 Downloaded:", filename)
    return path

# ---------------- DMS UPLOAD ----------------

def upload_to_dms(file_path, metadata):

    filename = os.path.basename(file_path)

    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    print("\n🚀 Uploading:", filename)
    print("📦 Payload:", json.dumps(metadata, indent=2))

    files = {
        "data": (None, json.dumps(metadata), "application/json"),
        "image": (filename, open(file_path, "rb"), mime)
    }

    headers = {
        "Authorization": DMS_AUTH
    }

    response = requests.post(
        DMS_URL,
        headers=headers,
        files=files,
        timeout=600
    )

    print("🔹 Status:", response.status_code)

    try:
        print("🔹 JSON:", json.dumps(response.json(), indent=2))
    except:
        print("🔹 Raw:", response.text)

    return response

# ---------------- MAIN ----------------

def main():

    sf = sf_login()
    mongo = mongo_connect()

    start = input("Start Date (YYYY-MM-DD): ")
    end = input("End Date (YYYY-MM-DD): ")

    query = f"""
    SELECT Id, Title, FileExtension, ContentSize,
           ContentDocumentId, CreatedDate,
           CreatedBy.Name
    FROM ContentVersion
    WHERE CreatedDate >= {start}T00:00:00Z
    AND CreatedDate <= {end}T23:59:59Z
    AND ContentSize > {MAX_SIZE}
    """

    print("\n🔍 Query:\n", query)

    records = sf.query_all(query)["records"]

    print("📊 Files Found:", len(records))

    for r in records:

        try:

            print("\n-----------------------------------")

            cv_id = r["Id"]
            title = r["Title"]
            ext = r["FileExtension"] or ""
            doc_id = r["ContentDocumentId"]

            filename = build_filename(title, ext)

            local_path = download_file(sf, cv_id, filename)

            size = os.path.getsize(local_path)

            checksum = generate_sha1(local_path)

            linked_id, obj = get_linked_entity(sf, doc_id)

            vertical = get_vertical(sf, linked_id, obj)

            agreement_no = get_agreement_number(sf, linked_id, obj)

            metadata = DMSRequestWrapper.create_upload_request(
                r, doc_id, size, checksum, vertical, agreement_no, ext, filename
            )

            res = upload_to_dms(local_path, metadata)

            status = "SuccessToDMS" if res.status_code in [200, 201] else "FailedToDMS"

            mongo.update_one(
                {"ContentDocumentId__c": doc_id},
                {"$set": {
                    "ContentDocumentId__c": doc_id,
                    "Document_Name__c": filename,
                    "Vertical__c": vertical,
                    "sObject_Name__c": obj,
                    "sObject_Record_Id__c": linked_id,
                    "DMS_Response__c": res.text,
                    "Migrate_Status__c": status,
                    "Checksum__c": checksum,
                    "File_Size": size,
                    "CreatedDate": datetime.now(timezone.utc)
                }},
                upsert=True
            )

            print("✅ Mongo Saved | Status:", status)

            if status == "SuccessToDMS":
                os.remove(local_path)
                print("🧹 File Deleted")

        except Exception as e:
            print("❌ ERROR:", str(e))

    print("\n🎉 PROCESS COMPLETED")

# ---------------- RUN ----------------

if __name__ == "__main__":
    main()
