import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime
from simple_salesforce import Salesforce
from pymongo import MongoClient

# ---------------- CONFIG ---------------- #
load_dotenv()

TEMP_DIR = "/Users/poornapriyak/Documents/DownloadContentFiles"

MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "salesforce_dms"
COLLECTION = "file_tracking"

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
# SF_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN")  # test or login

DMS_ENDPOINT = os.getenv("DMS_ENDPOINT")
DMS_METHOD = os.getenv("DMS_METHOD", "POST")
DMS_TIMEOUT = int(os.getenv("DMS_TIMEOUT", 120))
DMS_HEADERS = json.loads(os.getenv("DMS_HEADERS"))["Headers"]

os.makedirs(TEMP_DIR, exist_ok=True)

# print(SF_USERNAME)
# print(SF_PASSWORD)
# # print(SF_TOKEN)

# ---------------- CONNECT ---------------- #
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=os.getenv("SF_SECURITY_TOKEN"),
    domain="test"   # sandbox
)

print("LOGIN OK ✅", sf.sf_instance)
print("✅ Salesforce connected")

mongo = MongoClient(MONGO_URI)
collection = mongo[DB_NAME][COLLECTION]

# ---------------- INPUT ---------------- #
start_date = input("Start Date (YYYY-MM-DD): ")
end_date = input("End Date (YYYY-MM-DD): ")

# ---------------- QUERY ---------------- #
query = f"""
SELECT
    Id,
    Title,
    ContentSize,
    ContentDocumentId,
    FileExtension,
    CreatedDate
FROM ContentVersion
WHERE ContentSize > 6000000
AND CreatedDate >= {start_date}T00:00:00Z
AND CreatedDate <= {end_date}T23:59:59Z
"""

files = sf.query_all(query)["records"]
print(f"Found {len(files)} eligible files")

# ---------------- PROCESS ---------------- #
for file in files:
    try:
        content_version_id = file["Id"]
        content_document_id = file["ContentDocumentId"]

        # ---- ContentDocumentLink ---- #
        cdl = sf.query(f"""
            SELECT LinkedEntityId
            FROM ContentDocumentLink
            WHERE ContentDocumentId = '{content_document_id}'
            LIMIT 1
        """)["records"]

        linked_entity_id = cdl[0]["LinkedEntityId"] if cdl else None
        sobject_name = linked_entity_id[:3] if linked_entity_id else None

        vertical = "UNKNOWN"
        if linked_entity_id:
            try:
                obj = sf.query(f"SELECT Vertical__c FROM {sobject_name} WHERE Id='{linked_entity_id}'")
                if obj["records"]:
                    vertical = obj["records"][0].get("Vertical__c", "UNKNOWN")
            except:
                pass

        # ---- Mongo check ---- #
        mongo_rec = collection.find_one({
            "content_version_id": content_version_id
        })

        if mongo_rec and mongo_rec.get("pushed_to_dms"):
            print(f"✔ Already pushed → {file['Title']}")
            continue

        # ---- Download File (FIXED URL) ---- #
        download_url = f"{sf.base_url}sobjects/ContentVersion/{content_version_id}/VersionData"
        headers = {"Authorization": f"Bearer {sf.session_id}"}

        response = requests.get(download_url, headers=headers)
        response.raise_for_status()

        file_name = f"{content_document_id}_{file['Title']}.{file['FileExtension']}"
        local_path = os.path.join(TEMP_DIR, file_name)

        with open(local_path, "wb") as f:
            f.write(response.content)

        # ---- Insert / Update Mongo ---- #
        collection.update_one(
            {"content_version_id": content_version_id},
            {"$set": {
                "content_document_id": content_document_id,
                "linked_entity_id": linked_entity_id,
                "sobject_name": sobject_name,
                "vertical": vertical,
                "local_path": local_path,
                "pushed_to_dms": False,
                "updated_at": datetime.utcnow()
            }},
            upsert=True
        )

        # ---- Push to DMS ---- #
        with open(local_path, "rb") as f:
            dms_response = requests.post(
    DMS_ENDPOINT,
    headers=DMS_HEADERS,
    files={"file": f},
    timeout=DMS_TIMEOUT
)

        if dms_response.status_code == 200:
            collection.update_one(
                {"content_version_id": content_version_id},
                {"$set": {
                    "pushed_to_dms": True,
                    "dms_pushed_at": datetime.utcnow()
                }}
            )

            os.remove(local_path)
            print(f"✔ Uploaded & deleted → {file['Title']}")
        else:
            print(f"✖ DMS failed → {file['Title']}")

    except Exception as e:
        print(f"✖ ERROR → {file['Title']} : {e}")

print("\n✔ Processing Completed")
