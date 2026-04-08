# ---------------- SALESFORCE (PROD) ----------------

SF_ACCESS_TOKEN=#00D2w000006VWRD!AQEAQCqEM5tqcOnJjbEIZLi9qde8KO80GoRiaqevX832IzAtORnw.fwVeGAwr_Xa5bTPcRSajajyZx3PBmpUPQSIFJ2iEYlC


SF_INSTANCE_URL=https://chola.my.salesforce.com


SF_CLIENT_ID=#3MVG9n_HvETGhr3CAoaAoEM4oG8B2KGquSaqDcwcKJQ_KBXF9hFWUa5m7G3hMv4rT_cScqDkUFeMaHqtRpZ7S
SF_CLIENT_SECRET=#5A010503763222E6946835DE4DBA63390E3326C9661FEBDC70072B82EEB520AF
SF_USERNAME=poornapriyak@chola.murugappa.com
SF_PASSWORD=Mar@@1712

SF_LOGIN_URL=https://login.salesforce.com

# ---------------- DMS ----------------

DMS_ENDPOINT=https://apieg.chola.murugappa.com/awsdms/1.0.0/dms/v1/image
DMS_METHOD=POST
DMS_TIMEOUT=120
DMS_BOUNDARY=1ff13444ed8140c7a32fc4e6451aa76d
DMS_AUTH_HEADER=Bearer eyJ4NXQiOiJOMkpqTWpOaU0yRXhZalJrTnpaalptWTFZVEF4Tm1GbE5qZzRPV1UxWVdRMll6YzFObVk1TlEiLCJraWQiOiJNREpsTmpJeE4yRTFPR1psT0dWbU1HUXhPVEZsTXpCbU5tRmpaalEwWTJZd09HWTBOMkkwWXpFNFl6WmpOalJoWW1SbU1tUTBPRGRpTkRoak1HRXdNQV9SUzI1NiIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhcGltYW5hZ2VyIiwiYXV0IjoiQVBQTElDQVRJT04iLCJhdWQiOiJBbmV3Z2J3WWtmZ3ZuRTlCMjdXZlVKTWhGX29hIiwibmJmIjoxNzQ5NDY2NjEzLCJhenAiOiJBbmV3Z2J3WWtmZ3ZuRTlCMjdXZlVKTWhGX29hIiwic2NvcGUiOiJkZWZhdWx0IiwiaXNzIjoiaHR0cHM6XC9cL2FwaWUuY2hvbGEubXVydWdhcHBhLmNvbTo0NDNcL29hdXRoMlwvdG9rZW4iLCJleHAiOjE3ODEwMjQyMTMsImlhdCI6MTc0OTQ2NjYxMywianRpIjoiN2FjYzk2NzctMTQwOC00Y2FlLWFjYzgtNGE0NWUzZWZhNGMwIn0.Vmw-WMyN37kyB8jbczAc4BNOeC49cdxQFsKMTrkLxCHUEh_7CdnJ7dAIVxiRAmiVn8A5vFLxlbSYGm2WMUaQIi-LqQ0K-efl5sJJc7HNi7TmpVA-YXshfPKyukrwtxLpP6kPPitk-jhNQo6TAUzZUNvSE6leXZhwokjhex9o1I4AeU5zgXqaECOwj_kwVQRuimALUECDylgHAVPAqhsylDI2P-hjkpkp1c2ipfO6GNEF6rhLwSmeYZQawamRdozN85UvoxpoQnpm67Bjpjvn-m7Q3FqmuspJxlTzOfjp3gqVbP2BP8nafX_-Zb_M0YuC13rpwq2myVbz5m92evMc6Q


# ---------------- MONGO ----------------

MONGO_CONNECTION_STRING=mongodb://devwrite1:devWrite1%40123@10.9.47.71:10051/?authSource=admin


#-------------------------------------------------------------

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
MAX_WORKERS = 5

# ---------------- LOAD ENV ----------------
load_dotenv()

SF_ACCESS_TOKEN = os.getenv("SF_ACCESS_TOKEN")
SF_INSTANCE_URL = os.getenv("SF_INSTANCE_URL")

SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_CLIENT_SECRET = os.getenv("SF_CLIENT_SECRET")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_LOGIN_URL = os.getenv("SF_LOGIN_URL")

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
    global SF_ACCESS_TOKEN, SF_INSTANCE_URL

    if SF_ACCESS_TOKEN and SF_INSTANCE_URL:
        try:
            sf = Salesforce(
                instance_url=SF_INSTANCE_URL,
                session_id=SF_ACCESS_TOKEN
            )
            sf.query("SELECT Id FROM User LIMIT 1")
            print("✅ Using existing token")
            return sf
        except:
            print("⚠️ Token expired, regenerating...")

    url = f"{SF_LOGIN_URL}/services/oauth2/token"

    payload = {
        "grant_type": "password",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
        "username": SF_USERNAME,
        "password": SF_PASSWORD
    }

    res = requests.post(url, data=payload)

    if res.status_code != 200:
        raise Exception(f"❌ Token generation failed: {res.text}")

    data = res.json()

    SF_ACCESS_TOKEN = data["access_token"]
    SF_INSTANCE_URL = data["instance_url"]

    print("✅ New token generated")

    return Salesforce(
        instance_url=SF_INSTANCE_URL,
        session_id=SF_ACCESS_TOKEN
    )

# ---------------- MONGO ----------------
def mongo_connect():
    client = MongoClient(MONGO_URI)
    db = client["DMS_TRACKING_DB"]
    col = db["VF_Prod_Check"]

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
def process_row(sf, mongo, row, cv_map):

    try:
        doc_id = row.get("ContentDocumentId")
        parent_id = row.get("LinkedEntityId")
        vertical = row.get("Vertical") or row.get("Vertical__c")

        if not doc_id:
            return "⚠️ Missing DocId"

        # -------- DUPLICATE CHECK --------
        existing = mongo.find_one({"ContentDocumentId__c": doc_id})

        if existing:
            status = existing.get("Migrate_Status__c")

            if status == "SuccessToDMS":
                msg = f"⏭️ SKIPPED (Duplicate - Already Success) {doc_id}"
                print(msg)
                return msg

        print(f"➡️ Processing {doc_id}")

        # -------- FETCH FROM BULK MAP --------
        cv = cv_map.get(doc_id)

        if not cv:
            return f"❌ No File {doc_id}"

        filename = cv["Title"]
        if cv.get("FileExtension"):
            filename += "." + cv["FileExtension"]

        path = os.path.join(DOWNLOAD_DIR, filename)

        # -------- DOWNLOAD --------
        if not os.path.exists(path):
            path = download_file(sf, cv["Id"], filename)

        size = os.path.getsize(path)
        checksum = generate_sha1(path)

        payload = build_dms_payload(cv, doc_id, size, checksum, vertical, filename)

        headers = {
            "Authorization": DMS_AUTH
        }

        # -------- SINGLE RETRY --------
        try:
            with open(path, "rb") as f:
                files = {
                    "data": (None, json.dumps(payload), "application/json"),
                    "image": (
                        filename,
                        f,
                        mimetypes.guess_type(path)[0] or "application/octet-stream"
                    )
                }

                res = session.post(DMS_URL, headers=headers, files=files, timeout=120)

            if res.status_code not in [200, 201]:
                print(f"⚠️ Retry once for {doc_id}")

                with open(path, "rb") as f:
                    files["image"] = (
                        filename,
                        f,
                        mimetypes.guess_type(path)[0] or "application/octet-stream"
                    )

                    res = session.post(DMS_URL, headers=headers, files=files, timeout=120)

        except Exception as e:
            return f"❌ ERROR {doc_id}: {str(e)}"

        status = "SuccessToDMS" if res.status_code in [200, 201] else "FailedToDMS"

        # -------- MONGO UPDATE --------
        with lock:
            mongo.update_one(
                {"ContentDocumentId__c": doc_id},
                {"$set": {
                    "ContentDocumentId__c": doc_id,
                    "Document_Name__c": filename,
                    "sObject_Record_Id__c": parent_id,
                    "Migrate_Status__c": status,
                    "Checksum__c": checksum,
                    "File_Size": size,
                    "DMS_Response__c": res.text,
                    "CreatedDate": datetime.now(timezone.utc)
                }},
                upsert=True
            )

        # -------- CLEANUP --------
        if status == "SuccessToDMS" and os.path.exists(path):
            os.remove(path)

        return f"✅ {status} {doc_id}"

    except Exception as e:
        return f"❌ ERROR {doc_id}: {str(e)}"

# ---------------- MAIN ----------------
def main():

    sf = sf_login()
    mongo = mongo_connect()

    csv_file = input("Enter CSV file path: ").strip().replace('"', '')

    with open(csv_file, mode="r", encoding="utf-8-sig") as file:
        rows = list(csv.DictReader(file))

    print(f"🚀 Processing {len(rows)} records with {MAX_WORKERS} threads")

    # -------- BULK FETCH --------
    doc_ids = list(set(
        row.get("ContentDocumentId")
        for row in rows if row.get("ContentDocumentId")
    ))

    cv_map = {}

    chunks = [doc_ids[i:i+100] for i in range(0, len(doc_ids), 100)]

    for chunk in chunks:
        query = f"""
        SELECT Id, Title, FileExtension, ContentSize,
               CreatedDate, CreatedBy.Name, ContentDocumentId
        FROM ContentVersion
        WHERE ContentDocumentId IN ({','.join([f"'{i}'" for i in chunk])})
        AND IsLatest = TRUE
        """

        results = sf.query_all(query)["records"]

        for rec in results:
            cv_map[rec["ContentDocumentId"]] = rec

    # -------- THREADING --------
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_row, sf, mongo, row, cv_map)
            for row in rows
        ]

        for future in as_completed(futures):
            print(future.result())

    print("\n🎉 BULK PROCESS COMPLETED")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()
