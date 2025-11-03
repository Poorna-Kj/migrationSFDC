
# #To run the job every two hours - 0 */2 * * * /usr/bin/python3 /path/to/integration.py >> /path/to/integration.log 2>&1


import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from collections import defaultdict
from simple_salesforce.exceptions import SalesforceMalformedRequest

# -----------------------------------
# Load Environment Variables
# -----------------------------------
load_dotenv()

MONGO_CONN_STR = os.getenv("MONGO_CONNECTION_STRING")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN", "login")

# -----------------------------------
# Connect to Salesforce and MongoDB
# -----------------------------------
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)
print("✅ Connected to Salesforce")

mongo_client = MongoClient(MONGO_CONN_STR)
db = mongo_client.get_database()

checkpoint_coll = db["_sync_metadata"]      # To track last sync timestamps
error_coll = db["_sync_errors"]             # To store error logs

# -----------------------------------
# Salesforce Object Configurations
# -----------------------------------
OBJECT_QUERIES = {
    "OContactRecording__c": """
        SELECT Id, Name, Agreement__c, Agreement__r.Name,
               Question1__c, Response1__c, Question2__c, Response2__c,
               SystemModstamp
        FROM OContactRecording__c 
    """,
     "OReceipt__c": """
        SELECT Id, Name, AgreementId__c, AgreementNo__c,
               Amount__c, CreatedDate, SystemModstamp
        FROM OReceipt__c
    """,
    "OApprovalRequest__c": """
        SELECT Id, Name, Agreement__c, ApprovalType__c, Status__c,
               SystemModstamp
        FROM OApprovalRequest__c
    """,
    "OChallan__c": """
        SELECT Id, Name, Amount__c, BankName__c, Status__c,
               SystemModstamp
        FROM OChallan__c
    """,
    "OReceiptBatch__c": """
        SELECT Id, Name, Challan__c, TotalAmount__c, TotalReceipts__c,
               Status__c, SystemModstamp
        FROM OReceiptBatch__c
    """,
    "OCollectionPayment__c": """
        SELECT Id, Name, AdviceAmount__c, AgreementNo__c,
               BatchId__c, ChargeAmount__c, Receipt__c,
               SystemModstamp
        FROM OCollectionPayment__c
    """
}

# -----------------------------------
# Helper Functions
# -----------------------------------
def get_last_sync(sobject_name):
    """Retrieve last successful sync timestamp from Mongo."""
    checkpoint = checkpoint_coll.find_one({"source": sobject_name})
    return checkpoint["last_sync_time"] if checkpoint else None


def update_checkpoint(sobject_name, newest_time):
    """Save latest SystemModstamp for incremental sync."""
    checkpoint_coll.update_one(
        {"source": sobject_name},
        {
            "$set": {
                "last_sync_time": newest_time,
                "last_run": datetime.now(timezone.utc)
            }
        },
        upsert=True,
    )


def log_error(sobject_name, record_id, stage, message, record_data=None):
    """Store detailed error logs in MongoDB."""
    error_doc = {
        "sobject_name": sobject_name,
        "record_id": record_id,
        "error_stage": stage,
        "error_message": str(message),
        "record_data": record_data or {},
        "timestamp": datetime.now(timezone.utc)
    }
    try:
        error_coll.insert_one(error_doc)
        print(f"⚠️ Logged error for {sobject_name} ({record_id}) at {stage}")
    except Exception as e:
        print(f"❌ Failed to write error log to MongoDB: {e}")

# -----------------------------------
# Core Sync Function
# -----------------------------------
def sync_salesforce_object(sobject_name, base_query):
    print(f"\n🚀 Syncing {sobject_name} ...")

    last_sync = get_last_sync(sobject_name)
    # Build SOQL query dynamically based on checkpoint
    if last_sync:
        soql = f"{base_query} WHERE SystemModstamp > {last_sync} ORDER BY SystemModstamp ASC LIMIT 50000"
    else:
        soql = f"{base_query} ORDER BY SystemModstamp ASC LIMIT 50000"

    try:
        result = sf.query_all(soql)
        records = result["records"]
        print(f"📦 Fetched {len(records)} records from {sobject_name}")
    except Exception as e:
        log_error(sobject_name, None, "Salesforce Query", e)
        print(f"❌ Failed to fetch records for {sobject_name}: {e}")
        return

    if not records:
        print(f"No new or updated records found for {sobject_name}.")
        return

    # Group by sObject_Record_Id__c (push all related records into one collection)
    grouped_records = defaultdict(list)
    for rec in records:
        rec.pop("attributes", None)
        group_id = rec.get("sObject_Record_Id__c") or "Collections"
        coll_name = f"{sobject_name}_{group_id}".replace(".", "_")
        grouped_records[coll_name].append(rec)

    total_upserts = 0
    newest_modstamp = None

    # Push to MongoDB per group
    for coll_name, docs in grouped_records.items():
        collection = db[coll_name]
        collection.create_index("Id", unique=True)
        ops = [
            UpdateOne({"Id": d["Id"]}, {"$set": d}, upsert=True)
            for d in docs
        ]
        try:
            result = collection.bulk_write(ops, ordered=False)
            count = (result.upserted_count or 0) + (result.modified_count or 0)
            total_upserts += count
            print(f"✅ {coll_name}: {count} records upserted in MongoDB")
        except Exception as e:
            for d in docs:
                log_error(sobject_name, d.get("Id"), "Mongo Upsert", e, d)

        # Track latest SystemModstamp
        latest_doc = max(d["SystemModstamp"] for d in docs if "SystemModstamp" in d)
        if newest_modstamp is None or latest_doc > newest_modstamp:
            newest_modstamp = latest_doc

    # Update checkpoint with last SystemModstamp
    if newest_modstamp:
        update_checkpoint(sobject_name, newest_modstamp)

    print(f"🔹 {sobject_name}: {total_upserts} records successfully processed.")

# -----------------------------------
# Main Execution
# -----------------------------------
if __name__ == "__main__":
    for obj_name, soql in OBJECT_QUERIES.items():
        try:
            sync_salesforce_object(obj_name, soql)
        except Exception as e:
            log_error(obj_name, None, "Main Sync Loop", e)

    print("\n🎯 All Salesforce objects synced successfully!")

****************************************************************************

import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from collections import defaultdict
from simple_salesforce.exceptions import SalesforceMalformedRequest

# -----------------------------------
# Load Environment Variables
# -----------------------------------
load_dotenv()

MONGO_CONN_STR = os.getenv("MONGO_CONNECTION_STRING")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN", "login")

# -----------------------------------
# Connect to Salesforce and MongoDB
# -----------------------------------
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)
print("✅ Connected to Salesforce")

mongo_client = MongoClient(MONGO_CONN_STR)
db = mongo_client.get_database()

checkpoint_coll = db["_sync_metadata"]      # To track last sync timestamps
error_coll = db["_sync_errors"]             # To store error logs

# -----------------------------------
# Salesforce Object Configurations
# -----------------------------------
OBJECT_QUERIES = {
    "OContactRecording__c": """
        SELECT Id, Name, Agreement__c, Agreement__r.Name,
               Question1__c, Response1__c, Question2__c, Response2__c,
               SystemModstamp
        FROM OContactRecording__c 
    """,
     "OReceipt__c": """
        SELECT Id, Name, AgreementId__c, AgreementNo__c,
               Amount__c, CreatedDate, SystemModstamp
        FROM OReceipt__c
    """,
    "OApprovalRequest__c": """
        SELECT Id, Name, Agreement__c, ApprovalType__c, Status__c,
               SystemModstamp
        FROM OApprovalRequest__c
    """,
    "OChallan__c": """
        SELECT Id, Name, Amount__c, BankName__c, Status__c,
               SystemModstamp
        FROM OChallan__c
    """,
    "OReceiptBatch__c": """
        SELECT Id, Name, Challan__c, TotalAmount__c, TotalReceipts__c,
               Status__c, SystemModstamp
        FROM OReceiptBatch__c
    """,
    "OCollectionPayment__c": """
        SELECT Id, Name, AdviceAmount__c, AgreementNo__c,
               BatchId__c, ChargeAmount__c, Receipt__c,
               SystemModstamp
        FROM OCollectionPayment__c
    """
}

# -----------------------------------
# Helper Functions
# -----------------------------------
def get_last_sync(sobject_name):
    """Retrieve last successful sync timestamp from Mongo."""
    checkpoint = checkpoint_coll.find_one({"source": sobject_name})
    return checkpoint["last_sync_time"] if checkpoint else None


def update_checkpoint(sobject_name, newest_time):
    """Save latest SystemModstamp for incremental sync."""
    checkpoint_coll.update_one(
        {"source": sobject_name},
        {
            "$set": {
                "last_sync_time": newest_time,
                "last_run": datetime.now(timezone.utc)
            }
        },
        upsert=True,
    )


def log_error(sobject_name, record_id, stage, message, record_data=None):
    """Store detailed error logs in MongoDB."""
    error_doc = {
        "sobject_name": sobject_name,
        "record_id": record_id,
        "error_stage": stage,
        "error_message": str(message),
        "record_data": record_data or {},
        "timestamp": datetime.now(timezone.utc)
    }
    try:
        error_coll.insert_one(error_doc)
        print(f"⚠️ Logged error for {sobject_name} ({record_id}) at {stage}")
    except Exception as e:
        print(f"❌ Failed to write error log to MongoDB: {e}")

# -----------------------------------
# Core Sync Function
# -----------------------------------
def sync_salesforce_object(sobject_name, base_query):
    print(f"\n🚀 Syncing {sobject_name} ...")

    last_sync = get_last_sync(sobject_name)
    # Build SOQL query dynamically based on checkpoint
    if last_sync:
        soql = f"{base_query} WHERE SystemModstamp > {last_sync} ORDER BY SystemModstamp ASC LIMIT 50000"
    else:
        soql = f"{base_query} ORDER BY SystemModstamp ASC LIMIT 50000"

    try:
        result = sf.query_all(soql)
        records = result["records"]
        print(f"📦 Fetched {len(records)} records from {sobject_name}")
    except Exception as e:
        log_error(sobject_name, None, "Salesforce Query", e)
        print(f"❌ Failed to fetch records for {sobject_name}: {e}")
        return

    if not records:
        print(f"No new or updated records found for {sobject_name}.")
        return

    # Group by sObject_Record_Id__c (push all related records into one collection)
    grouped_records = defaultdict(list)
    for rec in records:
        rec.pop("attributes", None)
        group_id = rec.get("sObject_Record_Id__c") or "Collections"
        coll_name = f"{sobject_name}_{group_id}".replace(".", "_")
        grouped_records[coll_name].append(rec)

    total_upserts = 0
    newest_modstamp = None

    # Push to MongoDB per group
    for coll_name, docs in grouped_records.items():
        collection = db[coll_name]
        collection.create_index("Id", unique=True)
        ops = [
            UpdateOne({"Id": d["Id"]}, {"$set": d}, upsert=True)
            for d in docs
        ]
        try:
            result = collection.bulk_write(ops, ordered=False)
            count = (result.upserted_count or 0) + (result.modified_count or 0)
            total_upserts += count
            print(f"✅ {coll_name}: {count} records upserted in MongoDB")
        except Exception as e:
            for d in docs:
                log_error(sobject_name, d.get("Id"), "Mongo Upsert", e, d)

        # Track latest SystemModstamp
        latest_doc = max(d["SystemModstamp"] for d in docs if "SystemModstamp" in d)
        if newest_modstamp is None or latest_doc > newest_modstamp:
            newest_modstamp = latest_doc

    # Update checkpoint with last SystemModstamp
    if newest_modstamp:
        update_checkpoint(sobject_name, newest_modstamp)

    print(f"🔹 {sobject_name}: {total_upserts} records successfully processed.")

# -----------------------------------
# Main Execution
# -----------------------------------
if __name__ == "__main__":
    for obj_name, soql in OBJECT_QUERIES.items():
        try:
            sync_salesforce_object(obj_name, soql)
        except Exception as e:
            log_error(obj_name, None, "Main Sync Loop", e)

    print("\n🎯 All Salesforce objects synced successfully!")


