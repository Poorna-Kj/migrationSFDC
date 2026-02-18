#Code to push files to dms and track in mongo

import os
import mimetypes
import requests
import json
import hashlib
from datetime import datetime, timezone
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING

# ================= ENV =================
load_dotenv()

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN    = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN   = os.getenv("SF_DOMAIN", "test")

DMS_URL     = os.getenv("DMS_ENDPOINT")
DMS_AUTH    = os.getenv("DMS_AUTH_HEADER")

MONGO_URI   = os.getenv("MONGO_CONNECTION_STRING")

MAX_SIZE = 6 * 1024 * 1024  # 6 MB

print("MONGO_URI USED:", MONGO_URI)

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
db = mongo_client["SalesforceExports"]
collection = db["file_tracking"]

collection.create_index(
    [("ContentDocumentId__c", ASCENDING)],
    unique=True
)

print("MongoDB Connected → salesforce_dms.file_tracking ✅")


# ================= MAIN =================
def main():

    sf = sf_login()

    start = input("Start Date (YYYY-MM-DD): ")
    end   = input("End Date (YYYY-MM-DD): ")

    query = f"""
    SELECT Id, Title, FileExtension, ContentSize, CreatedDate,
           CreatedBy.Name,
           Owner.Name,
           ContentDocumentId
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
            ext = r["FileExtension"] or ""
            content_document_id = r["ContentDocumentId"]
            owner_name = r["Owner"]["Name"]

            # ================= GET LINKED ENTITY =================
            cdl_query = f"""
            SELECT LinkedEntityId, LinkedEntity.Type
            FROM ContentDocumentLink
            WHERE ContentDocumentId = '{content_document_id}'
            LIMIT 1
            """

            cdl_result = sf.query(cdl_query)

            if cdl_result["records"]:
                linked_entity_id = cdl_result["records"][0]["LinkedEntityId"]
                sobject_type = cdl_result["records"][0]["LinkedEntity"]["Type"]
            else:
                linked_entity_id = None
                sobject_type = None

            # ================= FILE NAME =================
            if ext and not title.lower().endswith(f".{ext.lower()}"):
                filename = f"{title}.{ext}"
            else:
                filename = title

            # ================= DOWNLOAD FILE =================
            download_url = f"{sf.base_url}sobjects/ContentVersion/{cv_id}/VersionData"

            sf_resp = requests.get(
                download_url,
                headers={"Authorization": f"Bearer {sf.session_id}"},
                timeout=300
            )
            sf_resp.raise_for_status()

            file_bytes = sf_resp.content
            checksum = generate_sha1(file_bytes)

            print(f"Downloaded → {filename} ({len(file_bytes)} bytes)")

            # ================= DATE FORMAT =================
            sf_date = r["CreatedDate"]
            parsed_date = datetime.strptime(sf_date[:10], "%Y-%m-%d")
            formatted_date = parsed_date.strftime("%d/%m/%Y")

            # ================= DMS METADATA (MATCHING YOUR WORKING PAYLOAD) =================
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
                "imageId": content_document_id,  # IMPORTANT
                "imageCategory": "0",
                "format": ext.lower(),
                "fileName": filename,
                "createdDate": formatted_date,
                "createdBy": r["CreatedBy"]["Name"],
                "checkSum": checksum,
                "branchCode": "1208",
                "branch": "HO",
                "appId": None
            }

            # ================= MULTIPART UPLOAD =================
            files = {
                "data": (None, json.dumps(metadata), "application/json"),
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
            dms_id = dms_resp.text.strip()
            dms_json = dms_resp.text


            print("STATUS:", dms_resp.status_code)
            print("BODY:", dms_resp.text)

            # ================= TRACKING =================
            if dms_resp.status_code in [200, 201]:

                tracking_doc = {
                    "ContentDocumentId__c": content_document_id,
                    "Document_Name__c": filename,
                    "sObject_Name__c": sobject_type,
                    "DMS_Id__c": dms_id,   # ✅ NOW CLEAN DMS ID
                    "DMS_Response__c": dms_json,  # ✅ FULL RESPONSE STORED

                    "Migrate_Status__c": "SuccessToDMS",
                    "DMS_Url__c": dms_resp.text,
                    "Push_to_DMS__c": True,
                    "sObject_Record_Id__c": linked_entity_id,
                    "IsUploadedToDMS__c": True,
                    "IsUploaded__c": True,
                    "Vertical__c": "VF_Gallop",
                    "Content_File_Owner__c": owner_name,
                    "Checksum__c": checksum,
                    "CreatedDate": datetime.now(timezone.utc)
                }

            else:

                tracking_doc = {
                    "ContentDocumentId__c": content_document_id,
                    "Document_Name__c": filename,
                    "sObject_Name__c": sobject_type,
                    "DMSId__c": None,
                    "Migrate_Status__c": "FailedToDMS",
                    "DMS_Url__c": None,
                    "Push_to_DMS__c": False,
                    "sObject_Record_Id__c": linked_entity_id,
                    "IsUploadedToDMS__c": False,
                    "IsUploaded__c": False,
                    "Vertical__c": "VF_Gallop",
                    "Content_File_Owner__c": owner_name,
                    "Checksum__c": checksum,
                    "Error_Message__c": dms_resp.text,
                    "CreatedDate": datetime.now(timezone.utc)
                }

            collection.update_one(
                {"ContentDocumentId__c": content_document_id},
                {"$set": tracking_doc},
                upsert=True
            )

            print("-" * 60)

        except Exception as e:

            print("ERROR:", str(e))

            collection.update_one(
                {"ContentDocumentId__c": r.get("ContentDocumentId")},
                {
                    "$set": {
                        "ContentDocumentId__c": r.get("ContentDocumentId"),
                        "Document_Name__c": r.get("Title"),
                        "Migrate_Status__c": "Exception",
                        "Error_Message__c": str(e),
                        "Push_to_DMS__c": False,
                        "IsUploadedToDMS__c": False,
                        "IsUploaded__c": False,
                        "CreatedDate": datetime.now(timezone.utc)
                    }
                },
                upsert=True
            )

    print("\n✔ Processing Completed")


if __name__ == "__main__":
    main()


