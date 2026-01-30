import os
from datetime import datetime, timezone
from pymongo import MongoClient
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
# Connect to Salesforce
# -----------------------------------
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)
print("✅ Connected to Salesforce")

# -----------------------------------
# Connect to MongoDB
# -----------------------------------
mongo_client = MongoClient(MONGO_CONN_STR)
db = mongo_client.get_database()
print(f"✅ Connected to MongoDB DB: {db.name}")

checkpoint_coll = db["_sync_metadata"]
error_coll = db["_sync_errors"]

# -----------------------------------
# Queries
# -----------------------------------
OBJECT_QUERIES = {
#     "OContactRecording__c": """
#         SELECT
#     Id,
#     Name,
#     ActionCode__c,
#     ActionCode__r.Name,
#     ActionCode__r.CreatedDate,
#     ActionCode__r.Days_allowed_from_Created_Date__c,
#     ActionCode__r.OptionType__c,
#     ActionCode__r.Task_Assign_To__c,
#     ActionCode__r.UserRole__c,
#     OwnerEmpNo__c,
#     Agreement__r.Name,
#     Agreement__r.Customer_Name__c,
#     Agreement__r.Status__c,
#     Agreement__r.NPASTAGE__c,
#     LastModifiedBy.Name,
#     Vertical__c,
#     SystemModstamp,
#     Question1__c,  Response1__c,
#     Question2__c,  Response2__c,
#     Question3__c,  Response3__c,
#     Question4__c,  Response4__c,
#     Question5__c,  Response5__c,
#     Question6__c,  Response6__c,
#     Question7__c,  Response7__c,
#     Question8__c,  Response8__c,
#     Question9__c,  Response9__c,
#     Question10__c, Response10__c,
#     Question11__c, Response11__c,
#     Question12__c, Response12__c,
#     Question13__c, Response13__c,
#     Question14__c, Response14__c,
#     Question15__c, Response15__c,
#     Question16__c, Response16__c,
#     Question17__c, Response17__c,
#     Question18__c, Response18__c,
#     Question19__c, Response19__c,
#     Question20__c, Response20__c,
#     Question21__c, Response21__c,
#     Question22__c, Response22__c,
#     Question23__c, Response23__c,
#     Question24__c, Response24__c,
#     Question25__c, Response25__c,
#     Question26__c, Response26__c,
#     Question27__c, Response27__c,
#     Question28__c, Response28__c,
#     Question29__c, Response29__c,
#     Question30__c, Response30__c,
#     Question31__c, Response31__c,
#     Question32__c, Response32__c,
#     Question33__c, Response33__c,
#     Question34__c, Response34__c,
#     Question35__c, Response35__c,
#     Question36__c, Response36__c,
#     Question37__c, Response37__c,
#     Question38__c, Response38__c,
#     Question39__c, Response39__c,
#     Question40__c, Response40__c,
#     Question41__c, Response41__c,
#     Question42__c, Response42__c,
#     Question43__c, Response43__c,
#     Question44__c, Response44__c,
#     Question45__c, Response45__c
# FROM OContactRecording__c
#         WHERE Migrated_to_Mongo__c = FALSE
#     """

#     ,
 
#     "OReceipt__c": """
#         SELECT Id,AgreementId__c, AgreementNo__c, AgreementNPAStage__c, AgreementNumber__c, Agreement_RefNumber__c,
# Amount__c, ApprovalFlowType__c, Approval_Ref_Number__c, Approver1__c, Approver11__c, Approver2__c, Approver21__c,
# Approver3__c, Approver31__c, Approver4__c, Approver41__c, Area__c, AutoAlloc__c, BankAccountNo__c, BankBranch__c,
# BankBranchID__c, BankName__c, BankID__c, BatchId__c, BBPSTxnId__c, BillingCity__c, BillingCountry__c,
# BillingPostalCode__c, BillingState__c, BillingStreet__c, BMGroupId__c, BounceDate__c, Branch__c, BranchId__c,
# Buyer__c, BuyerId__c, CardApprovalCode__c, CardCategory__c, CardExpiryDate__c, CardHolderName__c,
# CardIssuerName__c, CardNumber__c, CardType__c, CFEUser__c, ChallanNo__c, ChallenId__c,
# ChequeDate__c, ChequeNo__c, ChequeStatus__c, ChequeAmount__c, ChequeId__c, CholaAccountNumber__c, CholaBank__c,
# CIFId__c, CityID__c, ClosedAgreementNo__c, CollectionPaymentJSON__c, ConsolidatedCollectionJSON__c,
# CreatedById, CreationDate__c, Customer__c, CustomerBankBranch__c, CustomerBankName__c, CustomerCIFId__c,
# CustomerName__c, DCRId__c, DelayReason__c, DepositDate__c, Description__c, DraftAmount__c,
# DrawnOn__c, EFTRefID__c, EFTStatus__c, Employee_Number__c, EmpowermentApprovalStatus__c,
# ExternalReceiptId__c, ExternalSystemDate__c, ExtractionDate__c, FinnoneBatchId__c,
# FlagStatus__c, FTAccountNo__c, FTMode__c, IFSCCode__c, IMD_AppID__c, IMD_ReferenceNo__c,
# imdreceiptdetails__c, InFavourOf__c, InstrumentDate__c, InstrumentNumber__c, IntCardBin__c,
# IntCardHolderName__c, IntCardNumber__c, IntCardType__c, IntDeviceSerialNumber__c,
# IntegratedTransactionLink__c, IntTxnMRN__c, IntTxnStatus__c, IntTxnType__c,
# IsHE_HLAPICalloutFailed__c, IsRazorpayQr__c, IsFileUploaded__c, isRPC__c, LastModifiedById,
# LMSChequeNo__c, LMSReceiptPushDescription__c, LMSReceiptPushStatus__c, LMSRejectReason__c,
# LMSRequestId__c, LMSStatus__c, LMSSyncDetails__c, LMSSyncStatus__c, LMSPaymentMode__c,
# Manual_Receipt_Deposit_Status__c, MICRCode__c, Migrated_to_Mongo__c, MyReceipts__c,
# NoofManualReceipts__c, OCustomer__c, Old_Receipt_Status__c, OReceipt_Created_Time__c,
# OtherChargesReason__c, OwnerId, OwnerBranch__c, OwnerDesignation__c, OwnerEmpNo__c, PANNumber__c,
# PayableAt__c, Payment_Gateway_Source__c, PaymentMode__c, PaymentStatus__c, Payment_Tracker__c,
# PaytmBankName__c, PaytmBankTxnId__c, PaytmGatewayName__c, PaytmOrderId__c, PaytmPaymentMode__c,
# PaytmResponseCode__c, PaytmResponseMessage__c, PaytmTransactionStatus__c, PaytmTxnAmount__c,
# PaytmTxnId__c, PaytmTxnStatus__c, PDCReasonDescription__c, PersonalBankAccountHolderName__c,
# PersonalBankAccountNo__c, PersonalBankIFSCCode__c, PersonalBankMICRNo__c, PersonalBankName__c,
# PersonalBranchName__c, PhoneNumber__c, Physical_Challan_No__c, PosResponseCode__c, PresentationDate__c,
# ProcessFlag__c, Product__c, ProductCode__c, ProposalId__c, PushtoGBStatus__c,
# RealizationDate__c, ReasonforDelay__c, ReasonId__c, ReceiptBatch__c, ReceiptBook__c,
# ReceiptCancelledDate__c, ReceiptDate__c, ReceiptDepositedStatus__c, ReceiptIndexId__c, Name,
# ReceiptNumber__c, ReceiptSMSId__c, ReceiptType__c, Recommender1__c, Recommender11__c,
# Recommender2__c, Recommender21__c, Recommender3__c, Recommender31__c, Recommender4__c,
# Recommender41__c, RecordTypeId, RecordTypes__c, ReferenceNumber__c, Region__c,
# RemarkforDelay__c, Remarks__c, RemitterName__c, Report_Status__c, RequestID__c,
# RetryCount__c, RuleOverride__c, RuleTemplate__c, SameSeriesChequeDetected__c, Source__c,
# SRNumber__c, Stage__c, Status__c, SubType__c, Towards__c,
# Transaction_Identifier__c, TransactionType__c, TXNIDRRN__c, Type__c, UploadDate__c,
# UserGroupName__c, UserID__c, UTRNumber__c, ValueDate__c, Vertical__c, Zone__c
# FROM OReceipt__c WHERE Migrated_to_Mongo__c = FALSE

# """

    
#     ,
 
#     "OApprovalRequest__c": """
#         SELECT Id, Name, AFC__c, Agreement__c, Agreement__r.AgreementNo__c, Agreement_number__c,
#                AllocatedCFEId__c, AlreadyRaisedReceipt__c, Amount__c, Approval_1_Emp_Number__c, ApprovalDate__c,
#                ApprovalExpiry__c, ApprovalFlowType__c, ApprovalRuleName__c, ApprovalType__c, Approved_By__c,
#                ApprovedBySecondaryApproval__c, ApprovedByUserId__c, Approver1__c, Approver1__r.EmployeeNumber,
#                AreaName__c, Branch__c, Branch_Name__c, BT_Banker__c, CBC__c, Closure_Reason__c, Closure_Type__c,
#                CollectionAmount__c, CollectionType__c, CreatedById, CreatedDate,
#                CrossSellCharges__c, CurrentRecommenderApprover__c, Customer_Profile_Previous_Auction__c,
#                CustomerBackground__c, CustomerName__c, CustomerNameText__c, DisbursedAmount__c, DMS_Ids__c,
#                DMSFile_Id__c, DPD__c, DRT_Amount__c, EditableFieldName__c, EMI__c, EmpowermentApprovalStatus__c,
#                FC_Excess_Amount__c, Fee_of_Legal_Action__c, Foreclosure_Waiver_Amount__c, Legal_Case_Agreement_No__c,
#                Legal_Case_Hold_Reason__c, Legal_Case_New_LookUp__c, Legal_Expense_Invoice_Amount_New_Value__c,
#                Legal_Expense_Invoice_Amount_Old_Value__c, OtherAmount__c, PaymentDetails__c, PaymentMethod__c,
#                PendingShortfall__c, ProductCode__c, Status__c, Stage__c, Initiator_EmpID__c, Receipt__c,
#                ReceiptBranch__c, Total_Charge_Due__c, Total_Charge_Waiver__c, Vertical__c,SystemModstamp
#         FROM OApprovalRequest__c WHERE Migrated_to_Mongo__c = FALSE
#     """,
 
#     "OChallan__c": """
#         SELECT Id, Name, Amount__c, ApprovalFlowType__c, Approver1__c, BankDepositedBatchesIncluded__c,
#                BankName__c, Branch__c, Challan_Overdue__c, ChallanDueDate__c, ChallanUploadDate__c, Challened_Date__c,
#                CholasACNumber__c, CreatedBy.EmployeeNumber, CreatedDate, DelayReason__c, EmpowermentApprovalStatus__c,
#                Entity__c, ExternalKey__c, ExternalSystemDate__c, IsEscalated__c, LastActivityDate, LastModifiedDate,
#                MyBranch__c, MyChallan__c, NumberOfReceipts__c, OwnerBranch__c, Owner.Name, OwnerDesignation__c, OwnerEmpNo__c,
#                OwnerId, PaymentMode__c, PhysicalChallanDate__c, PhysicalChallanNo__c, ProductCode__c, ReasonforEscalation__c,
#                Recommender1__c, RecordType.DeveloperName, Remarks__c, Source__c, Stage__c, Status__c, Teller__c,
#                TotalNumberofDaysDelayed__c, Type__c, UserGroupName__c, Vertical__c,SystemModstamp
#         FROM OChallan__c WHERE Migrated_to_Mongo__c = FALSE
#     """,
 
#     "OReceiptBatch__c": """
#         SELECT Id, Name, BankDepositedBatch__c, Challan__c, CreatedBy.EmployeeNumber, CreatedDate, ExternalKey__c,
#                HandOffApprovedDate__c, HandOffDate__c, HandOffStatus__c, HandOffTo__c, HandOffToBranch__c, HandsOffToUserDetail__c,
#                LastActivityDate, Owner.Name, OwnerBranch__c, OwnerDesignation__c, OwnerEmpNo__c, PaymentMode__c, ProductCode__c,
#                RecordType.DeveloperName, Source__c, Status__c, TotalAmount__c, TotalReceipts__c, TransactionId__c, Type__c,
#                UserBranch__c, UserBranch__r.BatchId__c, UserGroupName__c, Vertical__c,SystemModstamp
#         FROM OReceiptBatch__c WHERE Migrated_to_Mongo__c = FALSE
#     """,
 
#     "OCollectionPayment__c": """
#         SELECT Id, Name, AdviceAmount__c, AgreementNo__c, BatchId__c, ChargeAmount__c,
#                ChargeId__c, ChargeName__c, ChargeTaxMaster__c, Cheque_Upload_Deadline_in_Days__c,
#                ChequeId__c, ClosedAgreementNo__c, CollectionExternalKey__c, CollectionType__c,
#                ExtractionDate__c, Included_for_Batching__c, Max_Amount_Allowed_in_Cash_Mode__c,
#                Max_Amount_CFE_Can_Carry__c, Max_Amount_Payable_for_Non_Agreement__c,
#                Max_No_of_Batches_Open_Per_CFE__c, Max_No_of_Cash_Receipts_Open_per_CFE__c,
#                Max_No_of_Cash_Receipts_Un_Batched__c, Max_No_of_Days_Cheque_Can_Be_Backdated__c,
#                Max_No_of_Days_POS_Can_Be_Backdated__c, Max_No_Of_Days_UnChallaned_Cash_Receipt__c,
#                Max_No_of_Un_Challaned_Batches_Per_User__c, Max_Shortfall_Amount_per_Agreement__c,
#                Min_Amount_For_PAN_Verification__c, Payment_Mode_Lap__c, PaymentId__c, Receipt__c,
#                RelatedReceipts__c, Vertical__c, WaiverAmount__c, LastModifiedDate, LastActivityDate,CreatedDate,SystemModstamp
#         FROM OCollectionPayment__c WHERE Migrated_to_Mongo__c = FALSE
#     """,
 
 
#     "ODMS_File__c": """
#         SELECT Id,Name,ContentDocumentId__c,CreatedDate,DMS_Url__c,DMSId__c,Document_Name__c,
#         Heap_Size_Issue__c,IsUploaded__c,IsUploadedToDMS__c,
#         LastModifiedDate,LastReferencedDate,LastViewedDate,Migrate_Status__c,Push_to_DMS__c,
#         sObject_Name__c,sObject_Record_Id__c,Vertical__c,OwnerId,Owner.Name ,SystemModstamp
#         FROM ODMS_File__c WHERE Migrated_to_Mongo__c = FALSE
#     """,
 
#     "ORelatedReceipts__c": """
# SELECT
#         Id,
#         Name,
#         AgreementNo__c,
#         ChequeId__c,
#         ClosedAgreementNo__c,
#         CreatedById,
#         CreatedDate,
#         IntegrationLogId__c,
#         LastModifiedById,
#         LastModifiedDate,
#         LMSRejectReason__c,
#         LMSRequestId__c,
#         LMSStatus__c,
#         LMSSyncDetails__c,
#         LMSSyncStatus__c,
#         LmsRequestIdFormula__c,
#         OwnerId,
#         Receipt__c,
#         RecordTypeId,
#         RelatedReceiptExternalKey__c,
#         RetryCount__c,
#         SystemModstamp,
#         Vertical__c
#     FROM ORelatedReceipts__c
#     WHERE Migrated_to_Mongo__c = FALSE
# """ ,
 
     "OExcessAmount__c": """
   SELECT
    Id,
    Name,
    AdviceDate__c,
    AdviceType__c,
    Agreement__c,
    AkoAmt__c,
    AkoFlag__c,
    BpId__c,
    BpType__c,
    ChargeId__c,
    CreatedById,
    CreatedDate,
    Description__c,
    ExcessAmount__c,
    LastModifiedById,
    LastModifiedDate,
    MkoAmt__c,
    MkoFlag__c,
    OwnerId,
    ProductId__c,
    Receipt__c,
    RecordTypeId,
    Remarks__c,
    Status__c,
    SystemModstamp,
    TxnAdjustedAmt__c,
    TxnAdviceId__c,
    Vertical__c
FROM OExcessAmount__c
"""
}
# -----------------------------------
# Helpers
# -----------------------------------
def get_last_sync(sobject_name):
    doc = checkpoint_coll.find_one({"source": sobject_name})
    print(f"DEBUG: Last sync for {sobject_name} → {doc}")
    return doc["last_sync_time"] if doc else None


def update_checkpoint(sobject_name, timestamp):
    print(f"DEBUG: Updating checkpoint → {timestamp}")
    checkpoint_coll.update_one(
        {"source": sobject_name},
        {"$set": {"last_sync_time": timestamp, "last_run": datetime.now(timezone.utc)}},
        upsert=True
    )


def log_error(sobject, record_id, stage, error, record=None):
    print(f"❌ ERROR [{stage}] → {record_id} → {error}")
    error_coll.insert_one({
        "sobject_name": sobject,
        "record_id": record_id,
        "stage": stage,
        "error": str(error),
        "record": record,
        "timestamp": datetime.now(timezone.utc)
    })


# -----------------------------------
# Core Sync
# -----------------------------------
def sync_salesforce_object(sobject_name, base_query):
    print(f"\n🚀 START SYNC → {sobject_name}")

    last_sync = get_last_sync(sobject_name)
    base_query_clean = base_query.strip()
    where_keyword = "AND" if "WHERE" in base_query_clean.upper() else "WHERE"

    if last_sync:
        sf_time = last_sync.strftime("%Y-%m-%dT%H:%M:%SZ")
        soql = f"{base_query_clean} {where_keyword} SystemModstamp > {sf_time} ORDER BY SystemModstamp ASC"
    else:
        soql = f"{base_query_clean} ORDER BY CreatedDate ASC"

    print("DEBUG: SOQL →")
    print(soql)

    result = sf.query(soql)
    print(f"DEBUG: Initial query result keys → {result.keys()}")

    total_processed = 0
    newest_modstamp = None

    while True:
        records = result.get("records", [])
        print(f"DEBUG: Records fetched in batch → {len(records)}")

        if not records:
            print("DEBUG: No records fetched, exiting loop")
            break

        grouped = defaultdict(list)

        # ----------------------------
        # Group by Vertical
        # ----------------------------
        for rec in records:
            rec.pop("attributes", None)

            vertical_raw = rec.get("Vertical__c")
            vertical = vertical_raw.strip().upper() if isinstance(vertical_raw, str) else "OTHER"
            if vertical not in {"SME", "LAP", "HL"}:
                vertical = "OTHER"

            coll_name = f"{sobject_name}_{vertical}"
            grouped[coll_name].append(rec)

        print(f"DEBUG: Grouped collections → {list(grouped.keys())}")

        # ----------------------------
        # Mongo Insert
        # ----------------------------
        for coll_name, docs in grouped.items():
            collection = db[coll_name]
            print(f"DEBUG: Processing Mongo collection → {coll_name}")

            # 🔍 Check existing indexes
            existing_indexes = collection.index_information()
            print(f"DEBUG: Existing indexes → {existing_indexes.keys()}")

            id_index_exists = False

            for idx_name, idx_info in existing_indexes.items():
                if idx_info.get("key") == [("Id", 1)]:
                    id_index_exists = True
                    print(f"DEBUG: Found existing Id index → {idx_name}")
                    break

            # ➕ Create index ONLY if missing
            if not id_index_exists:
                print(f"DEBUG: Creating unique Salesforce Id index on {collection.name}")
                collection.create_index(
                    [("Id", 1)],
                    unique=True,
                    name="sf_id_unique_idx"
                )
            else:
                print(f"DEBUG: Salesforce Id index already exists on {collection.name}")

            # 🚀 Proceed with upsert logic
            sf_ids_to_update = []
            
            for rec in docs:
                sf_id = rec["Id"]
                print(f"DEBUG: Processing SF record → {sf_id}")

                existing = collection.find_one({"_id": sf_id})
                if existing:
                    print(f"DEBUG: Record already exists in Mongo → {sf_id}")
                    if existing.get("Migrated_to_Mongo__c"):
                        print("DEBUG: Skipping already migrated record")
                        continue

                rec["_id"] = sf_id
                rec["Migrated_to_Mongo__c"] = True
                rec["Migrated_On"] = datetime.utcnow()

                collection.update_one(
                    {"_id": sf_id},
                    {"$set": rec},
                    upsert=True
                )

                print(f"DEBUG: Upserted into Mongo → {sf_id}")
                sf_ids_to_update.append(sf_id)

            print(f"DEBUG: Salesforce IDs to update → {sf_ids_to_update}")

            # ----------------------------
            # Salesforce PATCH
            # ----------------------------
            for i in range(0, len(sf_ids_to_update), 200):
                chunk = sf_ids_to_update[i:i + 200]
                print(f"DEBUG: Salesforce PATCH chunk → {chunk}")

                payload = {
                    "allOrNone": False,
                    "records": [
                        {"attributes": {"type": sobject_name}, "Id": rid, "Migrated_to_Mongo__c": True}
                        for rid in chunk
                    ]
                }

                print("DEBUG: PATCH payload →")
                print(payload)

                response = sf.restful(
                    "composite/sobjects",
                    method="PATCH",
                    data=payload
                )

                print("DEBUG: Salesforce PATCH response →")
                print(response)

                for res in response:
                    if not res.get("success"):
                        log_error(
                            sobject_name,
                            res.get("id"),
                            "Salesforce PATCH",
                            res.get("errors"),
                            payload
                        )

            total_processed += len(sf_ids_to_update)

        batch_latest = max(
            (r.get("SystemModstamp") for r in records if r.get("SystemModstamp")),
            default=None
        )

        print(f"DEBUG: Batch latest SystemModstamp → {batch_latest}")

        if batch_latest:
            newest_modstamp = batch_latest

        if result.get("done"):
            print("DEBUG: Salesforce pagination done")
            break

        result = sf.query_more(result["nextRecordsUrl"], True)

    if newest_modstamp:
        update_checkpoint(sobject_name, newest_modstamp)

    print(f"✅ COMPLETED {sobject_name} → {total_processed} records migrated")


# -----------------------------------
# Main
# -----------------------------------
if __name__ == "__main__":
    for obj, query in OBJECT_QUERIES.items():
        try:
            sync_salesforce_object(obj, query)
        except Exception as e:
            log_error(obj, None, "Main Loop", e)

    print("\n🎉 All Salesforce objects migrated successfully!")
