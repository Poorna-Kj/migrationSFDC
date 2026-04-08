import os
import csv
import json
import hashlib
import mimetypes
import requests
import threading
 
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
 
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from pymongo import MongoClient, ASCENDING
 
# ---------------- CONFIG ----------------
MAX_WORKERS = 5   # 🔥 Tune carefully (5 → 10 → 15)
 
# ---------------- LOAD ENV ----------------
load_dotenv()
 
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN")
 
DMS_URL = os.getenv("DMS_ENDPOINT")
DMS_AUTH = os.getenv("DMS_AUTH_HEADER")
 
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
 
DOWNLOAD_DIR = "retry_files"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
 
# ---------------- GLOBALS ----------------
lock = threading.Lock()
session = requests.Session()
 
# ---------------- SALESFORCE ----------------
def sf_login():
    sf = Salesforce(
        username=SF_USERNAME,
        password=SF_PASSWORD,
        security_token=SF_TOKEN,
        domain=SF_DOMAIN
    )
    print("✅ Salesforce Connected:", sf.sf_instance)
    return sf
 
# ---------------- MONGO ----------------
def mongo_connect():
    client = MongoClient(MONGO_URI)
    db = client["DMS_TRACKING_DB"] #Don't change DB
    col = db["DMS_HL_COLLECTION_FILE_METADATA"] #collection name
 
    col.create_index([("ContentDocumentId__c", ASCENDING)], unique=True)
 
    print("✅ Mongo Connected")
    return col
 
# ---------------- HASH ----------------
def generate_sha1(file_path):
    sha1 = hashlib.sha1()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha1.update(chunk)
    return sha1.hexdigest()
 
# ---------------- DOWNLOAD ----------------
def download_file(sf, version_id, filename):
    url = f"{sf.base_url}sobjects/ContentVersion/{version_id}/VersionData"
    headers = {"Authorization": f"Bearer {sf.session_id}"}
 
    path = os.path.join(DOWNLOAD_DIR, filename)
 
    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
 
    return path
 
# ---------------- DMS PAYLOAD ----------------
def build_dms_payload(cv, doc_id, size, checksum, vertical, filename):
 
    created_date = datetime.strptime(
        cv["CreatedDate"][:10], "%Y-%m-%d"
    ).strftime("%d/%m/%Y")
 
    return {
        "imageId": doc_id,
        "uniqueId": cv["Id"],
        "sourceSys": "Gallop",
        "branchCode": "1208",
        "branch": "HO",
        "vertical": vertical or "Unknown",
        "stage": "FI",
        "module": "Notice Letters",
        "imageCategory": "0",
        "imageSubCategory": "0",
        "status": "1",
        "fileName": filename,
        "latLong": "242-12344",
        "imageSrc": "Gallop",
        "imageSrcId": "01",
        "format": (cv.get("FileExtension") or "").lower(),
        "user": cv["CreatedBy"]["Name"],
        "size": str(size),
        "createdBy": cv["CreatedBy"]["Name"],
        "createdDate": created_date,
        "checkSum": checksum,
        "keyId": ["agreementNo"],
        "keyValue": ["UNKNOWN"]
    }
 
# ---------------- PROCESS ROW ----------------
def process_row(sf, mongo, row):
 
    try:
        doc_id = row.get("ContentDocumentId")
        parent_id = row.get("LinkedEntityId")
        vertical = row.get("Vertical") or row.get("Vertical__c")
 
        if not doc_id:
            return "⚠️ Missing DocId"
 
        print(f"➡️ Processing {doc_id}")
 
        # -------- DUPLICATE CHECK --------
        existing = mongo.find_one({"ContentDocumentId__c": doc_id})
 
        if existing:
            status = existing.get("Migrate_Status__c")
            if status == "SuccessToDMS":
                return f"⏭️ Skipped (Already Success) {doc_id}"
 
        # -------- FETCH FILE --------
        cv_res = sf.query(f"""
            SELECT Id, Title, FileExtension, ContentSize,
                   CreatedDate, CreatedBy.Name
            FROM ContentVersion
            WHERE ContentDocumentId = '{doc_id}'
            AND IsLatest = TRUE
            LIMIT 1
        """)
 
        if not cv_res["records"]:
            return f"❌ No File {doc_id}"
 
        cv = cv_res["records"][0]
 
        # 🔥 UNIQUE FILENAME (CRITICAL FIX)
        # filename = f"{doc_id}_{cv['Title']}"
        filename=cv['Title']
        if cv.get("FileExtension"):
            filename += "." + cv["FileExtension"]
 
        path = os.path.join(DOWNLOAD_DIR, filename)
 
        # -------- DOWNLOAD (Retry Safe) --------
        if not os.path.exists(path):
            try:
                path = download_file(sf, cv["Id"], filename)
            except Exception as e:
                return f"❌ Download Failed {doc_id}: {str(e)}"
 
        # -------- FILE CHECK --------
        if not os.path.exists(path):
            return f"❌ File Missing After Download {doc_id}"
 
        size = os.path.getsize(path)
        checksum = generate_sha1(path)
 
        payload = build_dms_payload(cv, doc_id, size, checksum, vertical, filename)
 
        # -------- RETRY (3 TIMES) --------
        res = None
        status = "FailedToDMS"
 
        for attempt in range(3):
            try:
                # 🔥 Re-check file before every attempt
                if not os.path.exists(path):
                    path = download_file(sf, cv["Id"], filename)
 
                with open(path, "rb") as f:
                    files = {
                        "data": (None, json.dumps(payload), "application/json"),
                        "image": (
                            filename,
                            f,
                            mimetypes.guess_type(path)[0] or "application/octet-stream"
                        )
                    }
 
                    res = session.post(
                        DMS_URL,
                        headers={"Authorization": DMS_AUTH},
                        files=files,
                        timeout=300
                    )
 
                if res.status_code in [200, 201]:
                    status = "SuccessToDMS"
                    break
                else:
                    print(f"⚠️ Attempt {attempt+1} failed for {doc_id}: {res.status_code}")
 
            except Exception as e:
                print(f"⚠️ Attempt {attempt+1} error for {doc_id}: {str(e)}")
 
                if attempt == 2:
                    return f"❌ ERROR {doc_id}: {str(e)}"
 
        # -------- MONGO UPDATE --------
        with lock:
            mongo.update_one(
                {"ContentDocumentId__c": doc_id},
                {"$set": {
                    "ContentDocumentId__c": doc_id,
                    "Document_Name__c": filename,
                    "Vertical__c": "HL", #Need to change Vertical accordingly
                    "sObject_Name__c": "OContactRecording__c", #Need to change objectName accordingly
                    "sObject_Record_Id__c": parent_id,
                    "DMS_Response__c": res.text if res else "ERROR",
                    "Migrate_Status__c": status,
                    "Checksum__c": checksum,
                    "File_Size": size,
                    "Retry_From_CSV": True,
                    "CreatedDate": datetime.now(timezone.utc)
                }},
                upsert=True
            )
 
        # -------- SAFE DELETE --------
        if status == "SuccessToDMS" and os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                print(f"⚠️ Delete failed {path}: {str(e)}")
 
        return f"✅ {status} {doc_id}"
 
    except Exception as e:
        return f"❌ ERROR {doc_id}: {str(e)}"
 
        
 
    # except Exception as e:
    #     return f"❌ ERROR {doc_id}: {str(e)}"
 
# ---------------- MAIN ----------------
def main():
 
    sf = sf_login()
    mongo = mongo_connect()
 
    csv_file = input("Enter CSV file path: ").strip().replace('"', '')
 
    with open(csv_file, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        rows = list(reader)
 
    print(f"🚀 Processing {len(rows)} records with {MAX_WORKERS} threads")
 
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_row, sf, mongo, row) for row in rows]
 
        for future in as_completed(futures):
            print(future.result())
 
    print("\n🎉 BULK PROCESS COMPLETED")
 
# ---------------- RUN ----------------
if __name__ == "__main__":
    main()




.env

#MONGO_CONNECTION_STRING=mongodb://devwrite1:devWrite1%40123@10.9.47.71:10051/SalesforceExports
MONGO_CONNECTION_STRING=mongodb://devwrite1:devWrite1%40123@10.9.47.71:10051/?authSource=admin
# MONGO_DB_NAME=SalesforceExports
# MONGO_COLLECTION_NAME=file_tracking
SF_USERNAME=gnaneswari@chola.murugapa.com.preprod02
SF_PASSWORD=Poorna@@0101
SF_SECURITY_TOKEN=Aj338ObgbZnbu21spJk1nH0h
SF_DOMAIN=test  # or test for sandbox 
#DMS_CONFIG
DMS_ENDPOINT=https://apieg.chola.murugappa.com/awsdms/1.0.0/dms/v1/image
DMS_METHOD=POST
DMS_TIMEOUT=120
DMS_BOUNDARY=1ff13444ed8140c7a32fc4e6451aa76d
DMS_AUTH_HEADER=Bearer eyJ4NXQJOMkpqTWpOaU0yRXhZalJrTnpaalptWTFZVEF4Tm1GbE5qZzRPV1UxWVdRMll6YzFObVk1TlEiLCJraWQiOiJNREpsTmpJeE4yRTFPR1psT0dWbU1HUXhPVEZsTXpCbU5tRmpaalEwWTJZd09HWTBOMkkwWXpFNFl6WmpOalJoWW1SbU1tUTBPRGRpTkRoak1HRXdNQV9SUzI1NiIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhcGltYW5hZ2VyIiwiYXV0IjoiQVBQTElDQVRJT04iLCJhdWQiOiJBbmV3Z2J3WWtmZ3ZuRTlCMjdXZlVKTWhGX29hIiwibmJmIjoxNzQ5NDY2NjEzLCJhenAiOiJBbmV3Z2J3WWtmZ3ZuRTlCMjdXZlVKTWhGX29hIiwic2NvcGUiOiJkZWZhdWx0IiwiaXNzIjoiaHR0cHM6XC9cL2FwaWUuY2hvbGEubXVydWdhcHBhLmNvbTo0NDNcL29hdXRoMlwvdG9rZW4iLCJleHAiOjE3ODEwMjQyMTMsImlhdCI6MTc0OTQ2NjYxMywianRpIjoiN2FjYzk2NzctMTQwOC00Y2FlLWFjYzgtNGE0NWUzZWZhNGMwIn0.Vmw-WMyN37kyB8jbczAc4BNOeC49cdxQFsKMTrkLxCHUEh_7CdnJ7dAIVxiRAmiVn8A5vFLxlbSYGm2WMUaQIi-LqQ0K-efl5sJJc7HNi7TmpVA-YXshfPKyukrwtxLpP6kPPitk-jhNQo6TAUzZUNvSE6leXZhwokjhex9o1I4AeU5zgXqaECOwj_kwVQRuimALUECDylgHAVPAqhsylDI2P-hjkpkp1c2ipfO6GNEF6rhLwSmeYZQawamRdozN85UvoxpoQnpm67Bjpjvn-m7Q3FqmuspJxlTzOfjp3gqVbP2BP8nafX_-Zb_M0YuC13rpwq2myVbz5m92evMc6Q

 
