
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
        SELECT Id, Name, ActionCode__c, OwnerEmpNo__c,ActionCode__r.Name,
               Agreement__r.Name, Agreement__r.Customer_Name__c,
               Agreement__r.Status__c, Agreement__r.NPASTAGE__c,
               LastModifiedBy.Name, Vertical__c,
               Question1__c, Response1__c, Question2__c, Response2__c,
               Question3__c, Response3__c, Question4__c, Response4__c,
               Question5__c, Response5__c, Question6__c, Response6__c,
               Question7__c, Response7__c, Question8__c, Response8__c, Question9__c, Response9__c,
               Question10__c, Response10__c, Question11__c, Response11__c, Question12__c, Response12__c,
               Question13__c, Response13__c, Question14__c, Response14__c, Question15__c, Response15__c,
               Question16__c, Response16__c, Question17__c, Response17__c, Question18__c, Response18__c,
               Question19__c, Response19__c, Question20__c, Response20__c, Question21__c, Response21__c,
               Question22__c, Response22__c, Question23__c, Response23__c, Question24__c, Response24__c,
               Question25__c, Response25__c, Question26__c, Response26__c, Question27__c, Response27__c,
               Question28__c, Response28__c, Question29__c, Response29__c, Question30__c, Response30__c,
               Question31__c, Response31__c, Question32__c, Response32__c, Question33__c, Response33__c,
               Question34__c, Response34__c, Question35__c, Response35__c, Question36__c, Response36__c,
               Question37__c, Response37__c, Question38__c, Response38__c, Question39__c, Response39__c,
               Question40__c, Response40__c, Question41__c, Response41__c, Question42__c, Response42__c,
               Question43__c, Response43__c, Question44__c, Response44__c, Question45__c, Response45__c
        FROM OContactRecording__c
        WHERE Agreement__r.Status__c = 'Closed'
          AND Agreement__r.NPASTAGE__c = 'REGULAR'
    """,

    "OReceipt__c": """
        SELECT Id, Name, AgreementId__c, AgreementNo__c, AgreementNo__r.AgreementId__c,
               AgreementNumber__c, Amount__c, Approval_Ref_Number__c, ApprovalFlowType__c, Approver1__c,
               Approver1__r.EmployeeNumber, Approver1__r.LastName, Area__c, AutoAlloc__c, BankAccountNo__c,
               BankBranch__c, BankBranchID__c, BankID__c, BankName__c, BatchId__c, BillingCity__c,
               BillingCountry__c, BillingPostalCode__c, BillingState__c, BillingStreet__c, BMGroupId__c,
               BounceDate__c, Branch__c, Branch__r.BranchCode__c, BranchId__c, CholaBank__c, CIFId__c,
               ChallanNo__c, ChallenId__c, ChequeId__c, Customer__c, CustomerName__c, OwnerEmpNo__c,
               IFSCCode__c, CustomerCIFId__c, OwnerDesignation__c, OwnerBranch__c, Description__c,
               DraftAmount__c, LMSChequeNo__c, LMSPaymentMode__c, CreatedDate, Vertical__c
        FROM OReceipt__c
    """,

    "OApprovalRequest__c": """
        SELECT Id, Name, AFC__c, Agreement__c, Agreement__r.AgreementNo__c, Agreement_number__c,
               AllocatedCFEId__c, AlreadyRaisedReceipt__c, Amount__c, Approval_1_Emp_Number__c, ApprovalDate__c,
               ApprovalExpiry__c, ApprovalFlowType__c, ApprovalRuleName__c, ApprovalType__c, Approved_By__c,
               ApprovedBySecondaryApproval__c, ApprovedByUserId__c, Approver1__c, Approver1__r.EmployeeNumber,
               AreaName__c, Branch__c, Branch_Name__c, BT_Banker__c, CBC__c, Closure_Reason__c, Closure_Type__c,
               CollectionAmount__c, CollectionType__c, CreatedById, CreatedDate,
               CrossSellCharges__c, CurrentRecommenderApprover__c, Customer_Profile_Previous_Auction__c,
               CustomerBackground__c, CustomerName__c, CustomerNameText__c, DisbursedAmount__c, DMS_Ids__c,
               DMSFile_Id__c, DPD__c, DRT_Amount__c, EditableFieldName__c, EMI__c, EmpowermentApprovalStatus__c,
               FC_Excess_Amount__c, Fee_of_Legal_Action__c, Foreclosure_Waiver_Amount__c, Legal_Case_Agreement_No__c,
               Legal_Case_Hold_Reason__c, Legal_Case_New_LookUp__c, Legal_Expense_Invoice_Amount_New_Value__c,
               Legal_Expense_Invoice_Amount_Old_Value__c, OtherAmount__c, PaymentDetails__c, PaymentMethod__c,
               PendingShortfall__c, ProductCode__c, Status__c, Stage__c, Initiator_EmpID__c, Receipt__c,
               ReceiptBranch__c, Total_Charge_Due__c, Total_Charge_Waiver__c, Vertical__c
        FROM OApprovalRequest__c
    """,

    "OChallan__c": """
        SELECT Id, Name, Amount__c, ApprovalFlowType__c, Approver1__c, BankDepositedBatchesIncluded__c,
               BankName__c, Branch__c, Challan_Overdue__c, ChallanDueDate__c, ChallanUploadDate__c, Challened_Date__c,
               CholasACNumber__c, CreatedBy.EmployeeNumber, CreatedDate, DelayReason__c, EmpowermentApprovalStatus__c,
               Entity__c, ExternalKey__c, ExternalSystemDate__c, IsEscalated__c, LastActivityDate, LastModifiedDate,
               MyBranch__c, MyChallan__c, NumberOfReceipts__c, OwnerBranch__c, Owner.Name, OwnerDesignation__c, OwnerEmpNo__c,
               OwnerId, PaymentMode__c, PhysicalChallanDate__c, PhysicalChallanNo__c, ProductCode__c, ReasonforEscalation__c,
               Recommender1__c, RecordType.DeveloperName, Remarks__c, Source__c, Stage__c, Status__c, Teller__c,
               TotalNumberofDaysDelayed__c, Type__c, UserGroupName__c, Vertical__c
        FROM OChallan__c
    """,

    "OReceiptBatch__c": """
        SELECT Id, Name, BankDepositedBatch__c, Challan__c, CreatedBy.EmployeeNumber, CreatedDate, ExternalKey__c,
               HandOffApprovedDate__c, HandOffDate__c, HandOffStatus__c, HandOffTo__c, HandOffToBranch__c, HandsOffToUserDetail__c,
               LastActivityDate, Owner.Name, OwnerBranch__c, OwnerDesignation__c, OwnerEmpNo__c, PaymentMode__c, ProductCode__c,
               RecordType.DeveloperName, Source__c, Status__c, TotalAmount__c, TotalReceipts__c, TransactionId__c, Type__c,
               UserBranch__c, UserBranch__r.BatchId__c, UserGroupName__c, Vertical__c
        FROM OReceiptBatch__c
    """,

    "OCollectionPayment__c": """
        SELECT Id, Name, AdviceAmount__c, AgreementNo__c, BatchId__c, ChargeAmount__c,
               ChargeId__c, ChargeName__c, ChargeTaxMaster__c, Cheque_Upload_Deadline_in_Days__c,
               ChequeId__c, ClosedAgreementNo__c, CollectionExternalKey__c, CollectionType__c,
               ExtractionDate__c, Included_for_Batching__c, Max_Amount_Allowed_in_Cash_Mode__c,
               Max_Amount_CFE_Can_Carry__c, Max_Amount_Payable_for_Non_Agreement__c,
               Max_No_of_Batches_Open_Per_CFE__c, Max_No_of_Cash_Receipts_Open_per_CFE__c,
               Max_No_of_Cash_Receipts_Un_Batched__c, Max_No_of_Days_Cheque_Can_Be_Backdated__c,
               Max_No_of_Days_POS_Can_Be_Backdated__c, Max_No_Of_Days_UnChallaned_Cash_Receipt__c,
               Max_No_of_Un_Challaned_Batches_Per_User__c, Max_Shortfall_Amount_per_Agreement__c,
               Min_Amount_For_PAN_Verification__c, Payment_Mode_Lap__c, PaymentId__c, Receipt__c,
               RelatedReceipts__c, Vertical__c, WaiverAmount__c, LastModifiedDate, LastActivityDate
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


