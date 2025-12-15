# Script to migrate Salesforce SARFAESI Legal Automation data to MongoDB

import os
from datetime import datetime
from simple_salesforce import Salesforce
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Salesforce Login
# ============================================================
sf = Salesforce(
    username=os.getenv("SF_USERNAME"),
    password=os.getenv("SF_PASSWORD"),
    security_token=os.getenv("SF_SECURITY_TOKEN"),
    domain=os.getenv("SF_DOMAIN")
)

# ============================================================
# MongoDB Connection
# ============================================================
mongo = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
db = mongo["salesforce_sarfeasi_legal_migration"]


# ============================================================
# Helper: Get Nested Field Value
# ============================================================
def get_nested_value(record, field_path):
    value = record
    for part in field_path.split("."):
        if isinstance(value, dict):
            value = value.get(part)
        else:
            return None
    return value


# ============================================================
# Generic Bulk Push Function (WITH ERROR TRACKING)
# ============================================================
def bulk_push(object_name, soql, vertical_field_path):

    print(f"\n==============================")
    print(f"Processing Object: {object_name}")
    print(f"==============================")

    records = sf.query_all(soql)["records"]
    print(f"Fetched records: {len(records)}")

    if not records:
        print("No records found. Skipping.")
        return

    vertical_map = {}
    sf_ids = []
    error_collection = db[f"{object_name}_ERRORS"]

    # -----------------------------
    # Group records by Vertical
    # -----------------------------
    for rec in records:
        try:
            vertical = get_nested_value(rec, vertical_field_path) or "Unknown"
            cleaned = {k: v for k, v in rec.items() if k != "attributes"}

            vertical_map.setdefault(vertical, []).append(cleaned)
            sf_ids.append(rec["Id"])

        except Exception as e:
            error_collection.insert_one({
                "object": object_name,
                "salesforce_id": rec.get("Id"),
                "stage": "GROUPING",
                "error": str(e),
                "record": rec,
                "timestamp": datetime.utcnow()
            })

    # -----------------------------
    # Insert into MongoDB
    # -----------------------------
    for vertical, rec_list in vertical_map.items():
        collection_name = f"{object_name}_{vertical}"
        collection = db[collection_name]

        try:
            collection.insert_many(rec_list, ordered=False)
            print(f"Inserted {len(rec_list)} records → {collection_name}")

        except Exception as e:
            error_collection.insert_one({
                "object": object_name,
                "vertical": vertical,
                "stage": "MONGO_INSERT",
                "error": str(e),
                "records_count": len(rec_list),
                "timestamp": datetime.utcnow()
            })

    # -----------------------------
    # Update Salesforce Flag (SAFE)
    # -----------------------------
    print("Updating Salesforce Migrated_to_Mongo__c flag...")

    chunk_size = 200
    sf_object = sf.__getattr__(object_name)

    for i in range(0, len(sf_ids), chunk_size):
        chunk = sf_ids[i:i + chunk_size]

        for rid in chunk:
            try:
                sf_object.update(rid, {
                    "Migrated_to_Mongo__c": True
                })
            except Exception as e:
                error_collection.insert_one({
                    "object": object_name,
                    "salesforce_id": rid,
                    "stage": "SALESFORCE_UPDATE",
                    "error": str(e),
                    "timestamp": datetime.utcnow()
                })

        print(f"Updated {len(chunk)} Salesforce records")

    print(f"✅ Migration completed for {object_name}")


# ============================================================
# ODMSFiles__c Migration
# ============================================================
odms_query = """
SELECT Id, Name, CIF_ID__c, ContentDocumentId__c, Contentversion_Id__c,
       DMSId__c, DMS_Url__c, Push_to_DMS__c, Heap_Size_Issue__c,
       Sarfaesi_Documents__c, Migrated_to_Mongo__c,
       ONotice__r.Vertical__c
FROM ODMSFiles__c
WHERE DMSId__c != null AND Migrated_to_Mongo__c = false
"""

bulk_push(
    object_name="ODMSFiles__c",
    soql=odms_query,
    vertical_field_path="ONotice__r.Vertical__c"
)


# ============================================================
# ONotice__c Migration
# ============================================================
notice_query = """
SELECT Id, Name, Affixation_Date__c, News_Paper_Eng__c,
       News_Paper_Vern__c, Publication_Date__c, Agreement_Num__c,
       Vertical__c, CIF_Id__c, CreatedById, LastModifiedById, OwnerId,
       Migrated_to_Mongo__c
FROM ONotice__c
WHERE Migrated_to_Mongo__c = false
"""

bulk_push(
    object_name="ONotice__c",
    soql=notice_query,
    vertical_field_path="Vertical__c"
)


# ============================================================
# ONotice_Details__c Migration
# ============================================================
notice_details_query = """
SELECT Id, Name, Notice_Id1__c,
       Notice_Id1__r.Agreement_Num__c,
       Notice_Id1__r.Vertical__c,
       Product_Type__c,
       RecordType.Name,
       RecordTypeId,
       Migrated_to_Mongo__c
FROM ONotice_Details__c
WHERE Migrated_to_Mongo__c = false
"""

bulk_push(
    object_name="ONotice_Details__c",
    soql=notice_details_query,
    vertical_field_path="Notice_Id1__r.Vertical__c"
)


# ============================================================
# OComment_History__c Migration
# ============================================================
comment_history_query = """
SELECT Id, Name, Comment_stage__c, ONotice_ID__c,
       Comments__c, CreatedBy.Name, CreatedDate,
       RecordTypeId, RecordType.Name,
       ONotice_ID__r.Vertical__c,
       Migrated_to_Mongo__c
FROM OComment_History__c
WHERE Migrated_to_Mongo__c = false
"""

bulk_push(
    object_name="OComment_History__c",
    soql=comment_history_query,
    vertical_field_path="ONotice_ID__r.Vertical__c"
)

print("\n✅ Salesforce → MongoDB Migration Completed Successfully")
