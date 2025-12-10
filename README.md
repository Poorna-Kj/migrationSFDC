******  Integration.py ******
✅ High-Level Summary 

This Python script performs incremental synchronization of multiple Salesforce objects into MongoDB, ensuring:

✔ Only unmigrated Salesforce records (Migrated_to_Mongo__c = FALSE) are fetched
✔ Only new/updated records (checked via SystemModstamp) are retrieved
✔ Records are grouped by Vertical (SME, LAP, HL, Other)
✔ Each group is stored in a separate MongoDB collection
✔ After successful MongoDB upsert → Salesforce records are marked as Migrated
✔ Old error logs for those successful records are deleted
✔ A checkpoint (last_sync_time) is stored to continue incremental sync next time

