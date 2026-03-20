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

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN")

DMS_URL = os.getenv("DMS_ENDPOINT")
DMS_AUTH = os.getenv("DMS_AUTH_HEADER")

MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")

DOWNLOAD_DIR = "large_files"
MAX_SIZE = 6 * 1024 * 1024

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# ---------------- OBJECT OPTIONS ----------------
SOBJECT_OPTIONS = [
    {"label": "OContactRecording", "value": "OContactRecording__c"},
    {"label": "OReceipt", "value": "OReceipt__c"},
    {"label": "OApprovalRequest", "value": "OApprovalRequest__c"},
    {"label": "OChallan", "value": "OChallan__c"},
    {"label": "OBatch", "value": "OReceiptBatch__c"},
    {"label": "ODMSFile", "value": "ODMSFiles__c"},
    {"label": "ContactRecording", "value": "ContactRecording__c"},
    {"label": "Receipt", "value": "Receipt__c"},
    {"label": "ApprovalRequestDetail", "value": "ApprovalRequestDetail__c"},
    {"label": "Challan", "value": "Challan__c"},
    {"label": "DocumentDetails", "value": "DocumentDetails__c"},
    {"label": "RelatedReceipts", "value": "RelatedReceipts__c"},
    {"label": "Repossesion", "value": "Repossesion__c"},
    {"label": "Valuation", "value": "Valuation__c"},
    {"label": "KYCDocument", "value": "KYCDocument__c"},
    {"label": "Legal Case", "value": "Legal_Case__c"},
    {"label": "Document Request", "value": "Document_Request__c"}
]

# ---------------- VF OBJECTS ----------------
VF_OBJECTS = {
    "ContactRecording__c",
    "Receipt__c",
    "ApprovalRequestDetail__c",
    "Challan__c",
    "DocumentDetails__c",
    "RelatedReceipts__c",
    "Repossesion__c",
    "Valuation__c",
    "KYCDocument__c",
    "Legal_Case__c",
    "Document_Request__c"
}

# ---------------- SELECT OBJECT ----------------
def select_sobject():
    print("\n📌 Select Object:\n")

    for i, obj in enumerate(SOBJECT_OPTIONS, start=1):
        print(f"{i}. {obj['label']} ({obj['value']})")

    while True:
        try:
            choice = int(input("\nEnter option number: "))
            if 1 <= choice <= len(SOBJECT_OPTIONS):
                selected = SOBJECT_OPTIONS[choice - 1]
                print(f"\n✅ Selected: {selected['label']} ({selected['value']})\n")
                return selected["value"]
        except:
            print("❌ Invalid input")

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
    db = client["DMS_TRACKING_DB"]
    col = db["DMS_FILE_METADATA"]

    col.create_index([("ContentDocumentId__c", ASCENDING)], unique=True)

    print("✅ MongoDB Connected")
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

# ---------------- DMS WRAPPER ----------------
class DMSRequestWrapper:

    @staticmethod
    def create(rec, size, checksum, vertical, agreement_no, filename):

        created_date = datetime.strptime(
            rec["ContentDocument"]["LatestPublishedVersion"]["CreatedDate"][:10],
            "%Y-%m-%d"
        ).strftime("%d/%m/%Y")

        return {
            "imageId": rec["ContentDocumentId"],
            "uniqueId": rec["Id"],
            "sourceSys": "Gallop",
            "branchCode": "1208",
            "branch": "HO",
            "vertical": vertical,
            "stage": "FI",
            "module": "Notice Letters",
            "imageCategory": "0",
            "imageSubCategory": "0",
            "status": "1",
            "fileName": filename,
            "latLong": "242-12344",
            "imageSrc": "Gallop",
            "imageSrcId": "01",
            "format": (rec["ContentDocument"]["LatestPublishedVersion"].get("FileExtension") or "").lower(),
            "user": rec["ContentDocument"]["LatestPublishedVersion"]["CreatedBy"]["Name"],
            "size": str(size),
            "createdBy": rec["ContentDocument"]["LatestPublishedVersion"]["CreatedBy"]["Name"],
            "createdDate": created_date,
            "checkSum": checksum,
            "keyId": ["agreementNo"],
            "keyValue": [agreement_no or "UNKNOWN"]
        }

# ---------------- DMS UPLOAD ----------------
def upload_to_dms(file_path, metadata):

    files = {
        "data": (None, json.dumps(metadata), "application/json"),
        "image": (
            os.path.basename(file_path),
            open(file_path, "rb"),
            mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        )
    }

    headers = {"Authorization": DMS_AUTH}

    print("⬆️ Uploading to DMS...")
    print("📡 Payload:", json.dumps(metadata, indent=2))

    res = requests.post(DMS_URL, headers=headers, files=files, timeout=600)

    print("📡 Status:", res.status_code)
    print("📡 Response:", res.text)

    return res

# ---------------- MAIN ----------------
def main():

    sf = sf_login()
    mongo = mongo_connect()

    start_date = input("Enter Start Date (YYYY-MM-DD): ")
    end_date = input("Enter End Date (YYYY-MM-DD): ")

    sObjectType = select_sobject()

    start_dt = f"{start_date}T00:00:00Z"
    end_dt = f"{end_date}T23:59:59Z"

    # ---------------- FETCH CDL ----------------
    query = f"""
    SELECT Id, ContentDocumentId, LinkedEntityId,
           ContentDocument.Title,
           ContentDocument.FileType,
           ContentDocument.LatestPublishedVersion.Id,
           ContentDocument.LatestPublishedVersion.ContentSize,
           ContentDocument.LatestPublishedVersion.FileExtension,
           ContentDocument.LatestPublishedVersion.CreatedDate,
           ContentDocument.LatestPublishedVersion.CreatedBy.Name
    FROM ContentDocumentLink
    WHERE LinkedEntityId IN (
        SELECT Id FROM {sObjectType}
        WHERE CreatedDate >= {start_dt}
        AND CreatedDate <= {end_dt}
    )
    """

    print("🔎 Fetching files...")
    records = sf.query_all(query)["records"]

    print("📂 Total Files:", len(records))

    large_files = [
        r for r in records
        if r["ContentDocument"]["LatestPublishedVersion"]["ContentSize"] > MAX_SIZE
    ]

    print("📦 Files >6MB:", len(large_files))

    # ---------------- PROCESS ----------------
    for rec in large_files:

        try:
            doc_id = rec["ContentDocumentId"]

            if mongo.find_one({"ContentDocumentId__c": doc_id}):
                print("⏭️ Already processed:", doc_id)
                continue

            version = rec["ContentDocument"]["LatestPublishedVersion"]
            version_id = version["Id"]

            filename = rec["ContentDocument"]["Title"]
            if version.get("FileExtension"):
                filename += "." + version["FileExtension"]

            print("\n⬇️ Download:", filename)

            path = download_file(sf, version_id, filename)

            size = os.path.getsize(path)
            checksum = generate_sha1(path)

            linked_id = rec["LinkedEntityId"]

            # -------- VERTICAL LOGIC --------
            if sObjectType in VF_OBJECTS:
                vertical = "VF"
                agreement_no = "UNKNOWN"
                print("📊 Vertical: VF (Hardcoded)")

            else:
                vertical = None
                agreement_no = "UNKNOWN"

                try:
                    obj_query = f"""
                    SELECT Vertical__c, Agreement_No__c
                    FROM {sObjectType}
                    WHERE Id = '{linked_id}'
                    LIMIT 1
                    """

                    res = sf.query(obj_query)

                    if res["records"]:
                        data = res["records"][0]
                        vertical = data.get("Vertical__c")
                        agreement_no = data.get("Agreement_No__c") or "UNKNOWN"

                        print(f"📊 Vertical Fetched: {vertical}")

                except Exception as e:
                    print("⚠️ Parent fetch failed:", str(e))

            # fallback
            if not vertical:
                vertical = "UNKNOWN"

            # -------- DMS --------
            metadata = DMSRequestWrapper.create(
                rec, size, checksum, vertical, agreement_no, filename
            )

            res = upload_to_dms(path, metadata)

            status = "SuccessToDMS" if res.status_code in [200, 201] else "FailedToDMS"

            # -------- MONGO --------
            mongo.update_one(
                {"ContentDocumentId__c": doc_id},
                {"$set": {
                    "ContentDocumentId__c": doc_id,
                    "Document_Name__c": filename,
                    "Vertical__c": vertical,
                    "sObject_Name__c": sObjectType,
                    "sObject_Record_Id__c": linked_id,
                    "DMS_Response__c": res.text,
                    "Migrate_Status__c": status,
                    "Checksum__c": checksum,
                    "File_Size": size,
                    "CreatedDate": datetime.now(timezone.utc)
                }},
                upsert=True
            )

            print("💾 Mongo Saved")

            if status == "SuccessToDMS":
                os.remove(path)
                print("🧹 Local file deleted")

            print("✅ Completed:", filename)

        except Exception as e:
            print("❌ ERROR:", str(e))

    print("\n🎉 ALL FILES PROCESSED")

# ---------------- RUN ----------------
if __name__ == "__main__":
    main()
