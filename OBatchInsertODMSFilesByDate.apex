/**
* @File Name : OBatchInsertODMSFilesByDate.cls
* @Description :
* @Author :
* @Last Modified By :
* @Last Modified On : September 7, 2025
* @Modification Log :
*==============================================================================
* Ver | Date | Author | Modification
*==============================================================================
* 1.0 | September 7, 2025 |   | Initial Version
**/

global class OBatchInsertODMSFilesByDate implements Database.Batchable<SObject>, Database.AllowsCallouts {

    global DateTime startDate;
    global DateTime endDate;
    global String sObjectType;

// Allowed object types
    private static final Set<String> ALLOWED_OBJECT_TYPES = 
        new Set<String>{'OContactRecording__c', 'OReceipt__c', 'OApprovalRequest__c','OChallan__c','OReceiptBatch__c'};


    global OBatchInsertODMSFilesByDate(DateTime startDate, DateTime endDate, String sObjectType) {
        this.startDate = startDate;
        this.endDate = endDate;
        this.sObjectType = sObjectType;

	}
global Database.QueryLocator start(Database.BatchableContext BC) {
        Set<Id> linkedIds = new Set<Id>();

        // 🔸 Filter only selected object type
        if (sObjectType == 'OContactRecording__c') {
            for (OContactRecording__c rec : [
                SELECT Id
                FROM OContactRecording__c
                WHERE CreatedDate >= :startDate
                  AND CreatedDate <= :endDate
            ]) {
                linkedIds.add(rec.Id);
            }
        }
        else if (sObjectType == 'OReceipt__c') {
            for (OReceipt__c rec : [
                SELECT Id
                FROM OReceipt__c
                WHERE CreatedDate >= :startDate
                  AND CreatedDate <= :endDate
            ]) {
                linkedIds.add(rec.Id);
            }
        }
        else if (sObjectType == 'OApprovalRequest__c') {
            for (OApprovalRequest__c rec : [
                SELECT Id
                FROM OApprovalRequest__c
                WHERE CreatedDate >= :startDate
                  AND CreatedDate <= :endDate
            ]) {
                linkedIds.add(rec.Id);
            }
        }
          else if (sObjectType == 'OChallan__c') {
            for (OChallan__c rec : [
                SELECT Id
                FROM OChallan__c
                WHERE CreatedDate >= :startDate
                  AND CreatedDate <= :endDate
            ]) {
                linkedIds.add(rec.Id);
            }
        }
         else if (sObjectType == 'OReceiptBatch__c') {
            for (OReceiptBatch__c rec : [
                SELECT Id
                FROM OReceiptBatch__c
                WHERE CreatedDate >= :startDate
                  AND CreatedDate <= :endDate
            ]) {
                linkedIds.add(rec.Id);
            }
        }
        else {
            System.debug('❌ Unsupported sObjectType: ' + sObjectType);
            return Database.getQueryLocator('SELECT Id FROM ContentDocumentLink WHERE Id = null');
        }

        if (linkedIds.isEmpty()) {
            System.debug('⚠️ No parent records found for ' + sObjectType);
            return Database.getQueryLocator('SELECT Id FROM ContentDocumentLink WHERE Id = null');
        }

        return Database.getQueryLocator([
            SELECT Id, ContentDocumentId,
            LinkedEntityId,
            LinkedEntity.Name,
            ContentDocument.Title,
            ContentDocument.FileType,
            ContentDocument.LatestPublishedVersion.Id,
            ContentDocument.LatestPublishedVersion.PathOnClient,
            ContentDocument.LatestPublishedVersion.FileExtension,
            ContentDocument.LatestPublishedVersion.ContentBodyId,
            ContentDocument.LatestPublishedVersion.CreatedBy.Name,
            ContentDocument.LatestPublishedVersion.ContentSize,
            ContentDocument.LatestPublishedVersion.CreatedDate,
            ContentDocument.LatestPublishedVersion.VersionData
        FROM ContentDocumentLink
            WHERE LinkedEntityId IN :linkedIds
        ]);
    }

  global void execute(Database.BatchableContext BC, List<SObject> scope) {
    // Step 1: Get all ContentDocumentIds in scope
    Set<Id> docIds = new Set<Id>();
    for (SObject sObj : scope) {
        ContentDocumentLink cdl = (ContentDocumentLink)sObj;
        docIds.add(cdl.ContentDocumentId);
    }

    // Step 2: Query already uploaded documents
    Set<Id> alreadyUploaded = new Set<Id>();
    for (ODMS_File__c file : [
        SELECT ContentDocumentId__c
        FROM ODMS_File__c
        WHERE ContentDocumentId__c IN :docIds
        AND Migrate_Status__c = 'Success'
    ]) {
        alreadyUploaded.add(file.ContentDocumentId__c);
    }

    // Step 3: Process only non-duplicates
    for (SObject sObj : scope) {
        ContentDocumentLink cdl = (ContentDocumentLink)sObj;

        if (alreadyUploaded.contains(cdl.ContentDocumentId)) {
            System.debug('Skipping duplicate ContentDocumentId: ' + cdl.ContentDocumentId);
            continue;
        }

        try {
            // Load VersionData
            ContentVersion cv = [
                SELECT VersionData
                FROM ContentVersion
                WHERE Id = :cdl.ContentDocument.LatestPublishedVersion.Id
                LIMIT 1
            ];
            cdl.ContentDocument.LatestPublishedVersion.VersionData = cv.VersionData;

            // Upload to DMS
            ODMSValidateFiles.uploadSingleFile(cdl);

        } catch (Exception e) {
            System.debug('Error during upload: ' + e.getMessage());
        }
    }
}


    global void finish(Database.BatchableContext BC) {
        System.debug('DMS batch process completed.');
    }
}
