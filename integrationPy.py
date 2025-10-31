import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne
from simple_salesforce import Salesforce
from dotenv import load_dotenv
from collections import defaultdict

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

mongo_client = MongoClient(MONGO_CONN_STR)
db = mongo_client.get_database()
checkpoint_coll = db["_sync_metadata"]
print(checkpoint_coll)
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
    """,
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
        print(f"Fetched {len(records)} records from {sobject_name}")
    except Exception as e:
        print(f"❌ Failed to fetch records for {sobject_name}: {e}")
        return

    if not records:
        print("No new or updated records found.")
        return

    # Group by sObject_Record_Id__c (push all related records into one collection)
    grouped_records = defaultdict(list)
    for rec in records:
        rec.pop("attributes", None)
        group_id = rec.get("sObject_Record_Id__c") or "default"
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
        if ops:
            result = collection.bulk_write(ops, ordered=False)
            count = (result.upserted_count or 0) + (result.modified_count or 0)
            total_upserts += count
            print(f"✅ {coll_name}: {count} upserts")

        # Track latest SystemModstamp
        latest_doc = max(d["SystemModstamp"] for d in docs if "SystemModstamp" in d)
        if newest_modstamp is None or latest_doc > newest_modstamp:
            newest_modstamp = latest_doc

    # Update checkpoint with last SystemModstamp
    if newest_modstamp:
        update_checkpoint(sobject_name, newest_modstamp)

    print(f"🔹 {sobject_name}: {total_upserts} records upserted.")


# -----------------------------------
# Run Sync for All Objects
# -----------------------------------
if __name__ == "__main__":
    for obj_name, soql in OBJECT_QUERIES.items():
        try:
            sync_salesforce_object(obj_name, soql)
        except Exception as e:
            print(f"❌ Error syncing {obj_name}: {e}")

    print("\n🎯 All Salesforce objects synced successfully!")
