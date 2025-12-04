/**
* @File Name : ODMSValidateFiles.cls
* @Description :
* @Author : Poorna Priya 
* @Last Modified By :
* @Last Modified On : June 24, 2025
* @Modification Log :
*==============================================================================
* Ver | Date | Author | Modification
*==============================================================================
* 1.0 | June 24, 2025 |  Poorna Priya | Initial Version
**/

public with sharing class ODMSValidateFiles {

 public static final Set<String> ALLOWED_OBJECTS = new Set<String>{'OReceipt__c', 'OChallan__c', 'OApprovalRequest__c', 'OContactRecording__c','OReceiptBatch__c' };
 //public static final Integer MAX_FILE_SIZE_BYTES = 6 * 1024 * 1024; // 12MB limit
public static final Integer MAX_FILE_SIZE_BYTES  = 12582912;

    public class DocumentFileWrapper {
        public Id contentDocumentId;
        public String title;
        public String fileName;
        public String fileExtension;
        public String fileMimeType;
        public Blob fileData;
        public String parentObjectType;
        public Id parentRecordId;
        public String parentRecordName;
        public Long fileSize;
        public Blob contentVersion;
        public String ownerName;

      public DocumentFileWrapper(ContentDocumentLink cdl) {
    this.contentDocumentId = cdl.ContentDocumentId;
    this.title = cdl.ContentDocument.Title;
    this.fileName = cdl.ContentDocument.LatestPublishedVersion.PathOnClient;
    this.fileExtension = cdl.ContentDocument.LatestPublishedVersion.FileExtension;
    this.fileMimeType = cdl.ContentDocument.FileType;
    this.parentRecordId = cdl.LinkedEntityId;
    this.parentObjectType = cdl.LinkedEntityId.getSObjectType().getDescribe().getName();
    this.parentRecordName = cdl.LinkedEntity.Name;
    this.contentVersion = cdl.ContentDocument.LatestPublishedVersion.VersionData;
    this.ownerName = cdl.ContentDocument.LatestPublishedVersion.CreatedBy.Name;      

    ContentVersion cvMeta = [
                SELECT ContentSize, Id,ContentBodyId,VersionData,Owner.Name
                FROM ContentVersion
                WHERE Id = :cdl.ContentDocument.LatestPublishedVersion.Id
                LIMIT 1
            ];
           this.fileSize = cvMeta.ContentSize;
          
             if (cvMeta.ContentSize <= MAX_FILE_SIZE_BYTES) {
                this.fileData = cvMeta.VersionData;
            } else {
                this.fileData = null; // Too large, skip upload
            }
}
    }
    
public static void uploadSingleFile(ContentDocumentLink cdl) {
     Map<String,OIntegrationInterface__mdt> mapServiceNameToIntegrationInterface = new Map<String,OIntegrationInterface__mdt>();
     Map<String,String> mapheader = new Map<String,String>();

    try {
        // Integer size = cdl.ContentDocument.LatestPublishedVersion.ContentSize;
                    ContentVersion cvSize = [
                SELECT ContentSize, VersionData, PathOnClient, FileExtension, CreatedBy.Name
                FROM ContentVersion
                WHERE ContentDocumentId = :cdl.ContentDocumentId
                AND IsLatest = true
                LIMIT 1
            ];

        Integer size = cvSize.ContentSize;
        System.debug('Content File Size  : '+size);
        if (size > MAX_FILE_SIZE_BYTES) {
            System.debug('File size exceeds limit. Skipping upload.');
            logHeapError(cdl, 'File too large for safe upload: ' + size);
            return;
        }

        // Extract file details
        String fileName = cdl.ContentDocument.LatestPublishedVersion.PathOnClient;
        String fileMimeType = cdl.ContentDocument.FileType;
        Blob fileBlob = cdl.ContentDocument.LatestPublishedVersion.VersionData;
        
        // *** CHANGED: Prevent heap issue BEFORE writeFile() ***
            if (fileBlob != null && fileBlob.size() > MAX_FILE_SIZE_BYTES) {
                System.debug('Blob size exceeds max limit before building multipart. Skipping upload.'); // *** CHANGED
                logHeapError(cdl, 'File size too large for safe heap upload: ' + fileBlob.size());
                return; // *** CHANGED
            }


        // Create the JSON request string for metadata (ensure this method exists and works)
        String reqJson = ODMSRequestWrapper.createUploadRequest(cdl, cdl.LinkedEntity.Name, cdl.ContentDocumentId);
        //System.debug('Request JSON: ' + reqJson);

        String contentType = OHttpFormBuilderForDMS_col.getContentType();

        // Start building form data string
        String form64 = '';
        form64 += OHttpFormBuilderForDMS_col.writeBoundary();
        form64 += OHttpFormBuilderForDMS_col.writeBodyParameter('data', reqJson);
        form64 += OHttpFormBuilderForDMS_col.writeBoundary();
        
        // *** CHANGED: Now safe to call writeFile() ***
            OHttpFormBuilderForDMS_col.writeFileResult fileResult =
                OHttpFormBuilderForDMS_col.writeFile('image', fileName, fileMimeType, fileBlob); // *** CHANGED

            if (fileResult == null || fileResult.Content == null) { // *** CHANGED
                System.debug('writeFile returned null due to size or validation. Skipping upload.'); // *** CHANGED
                return; // *** CHANGED
            }
        form64 += fileResult.Content;
        form64 += OHttpFormBuilderForDMS_col.writeBoundary(fileResult.EndingType);

        //Blob formBlob = Blob.valueOf(form64);
        blob formBlob = EncodingUtil.base64Decode(form64);

        // Validate total multipart body size < 12 MB
Integer contentLengthHeader = formBlob.size();

if (contentLengthHeader > MAX_FILE_SIZE_BYTES) {
    System.debug('Multipart body exceeds max 12 MB limit. Skipping upload.');
    return; 
}

        // Calculate content length for headers
        String contentLength = String.valueOf(formBlob.size());

        // Optionally calculate checksum if needed
        String checksum = calculateCheckSum(fileBlob);
        //System.debug('Checksum: ' + checksum);
        mapServiceNameToIntegrationInterface = OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload');
        String apiType = mapServiceNameToIntegrationInterface.get('LAP_DMSFileUpload').DeveloperName;
        mapheader.put('Content-Type',contentType);
        mapheader.put('Connection', 'keep-alive');
        mapheader.put('Content-Length', contentLength);
        mapheader.put('Cache-Control', 'no-cache');        

        // Get headers from metadata and add necessary headers
        Map<String, String> headers = OCustomMetadataUtility_cmn.getHeaders(
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Headers__c
        );

        headers.put('Content-Type', contentType);
        headers.put('Content-Length', contentLength);
        headers.put('Connection', 'keep-alive');
        headers.put('Cache-Control', 'no-cache');

     
        // Make HTTP callout
        String dmsResponse = callout(
            null,
            formBlob,
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Endpoint__c,
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Method__c,
            headers,
            Integer.valueOf(OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Timeout__c),
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').CallbackClassName__c,
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').CallbackMethodName__c,
            new List<String>{String.valueOf(cdl.LinkedEntityId)},
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Integration_Type__c,
            OCustomMetadataUtility_cmn.getAPIDetails('LAP_DMSFileUpload').get('LAP_DMSFileUpload').Service_Provider__c,
            cdl.LinkedEntityId
        );

        if (dmsResponse != null) {
            String sObjectType = cdl.LinkedEntityId.getSObjectType().getDescribe().getName();
            String verticalValue;
            try {
                // Correct: concatenate the Id safely into the dynamic SOQL
                String soql = 'SELECT Vertical__c FROM ' + sObjectType + ' WHERE Id = \'' + String.escapeSingleQuotes(String.valueOf(cdl.LinkedEntityId)) + '\' LIMIT 1';
                SObject parentRecord = Database.query(soql);

                verticalValue = (String) parentRecord.get('Vertical__c');
                //System.debug('Fetched Vertical__c for ' + sObjectType + ': ' + verticalValue);
            } catch (Exception ex) {
                System.debug('No Vertical__c found for ' + sObjectType + ': ' + ex.getMessage());
                verticalValue = null;
            }
            ContentVersion cv = [
                                SELECT OwnerId, Owner.Name
                                FROM ContentVersion
                                WHERE ContentDocumentId = :cdl.ContentDocumentId
                                AND IsLatest = true
                                LIMIT 1
                            ];
            insert new ODMS_File__c (
                ContentDocumentId__c = cdl.ContentDocumentId,
                Document_Name__c = fileName,
                sObject_Name__c = sObjectType,
                DMSId__c =dmsResponse,
                Migrate_Status__c = 'SuccessToDMS',
                DMS_Url__c = dmsResponse,
                Push_to_DMS__c = true,
                sObject_Record_Id__c = cdl.LinkedEntityId,
                IsUploadedToDMS__c =true,
                IsUploaded__c =true,
                Vertical__c = verticalValue,
                Content_File_Owner__c = cv.Owner.Name
            );
        }
        else
        {
                    logHeapError(cdl, 'Exception during upload: ');
        }
    } catch (Exception e) {
        System.debug('uploadSingleFile error: ' + e.getMessage());
        logHeapError(cdl, 'Exception during upload: ' + e.getMessage());

    }
    
}
    
private static void logHeapError(ContentDocumentLink cdl, String message) {
    System.debug('Heap/Error log for ContentDocumentId ' + cdl.ContentDocumentId + ': ' + message);
    try { 
        insert new ODMS_HeapError__c( ContentDocumentId__c = cdl.ContentDocumentId, 
                                     FileName__c = cdl.ContentDocument.LatestPublishedVersion.PathOnClient, 
                                     ParentRecordId__c = cdl.LinkedEntityId, 
                                     ErrorMessage__c = message,
                                     FileSize__c = cdl.ContentDocument.LatestPublishedVersion.ContentSize ); 
    } 
    catch (Exception ex) 
    {
        System.debug('Failed to insert ODMS_HeapError__c: ' + ex.getMessage()); 
    }
}

  
@TestVisible
    // Step 8: Mark parent as uploaded
   private static void updateFlagIfAllowed(Id recordId) {
        String sObjectType = recordId.getSObjectType().getDescribe().getName();

        if (!ALLOWED_OBJECTS.contains(sObjectType)) return;

        try {
            SObject recordToUpdate = (SObject) Type.forName('Schema', sObjectType).newInstance();
            recordToUpdate.put('Id', recordId);
            recordToUpdate.put('Ready_For_DMS_Upload__c', true);
            update recordToUpdate;
        } catch (Exception e) {
            System.debug('Failed to update Ready_For_DMS_Upload__c: ' + e.getMessage());
        }
    }

      
    public static String callout(String textBody, Blob blobBody, String endPoint, String method, Map<String,String> headers, Integer timeout, String callbackClass, String callbackMethod, List<String> recordIds, String integrationType, String serviceProvider, String recordId) {
        //System.debug('Calling out...');
        Http http = new Http();
        HttpRequest req = new HttpRequest();
        req.setEndpoint(endPoint);
        req.setMethod(method);
        req.setTimeout(timeout);
        if (textBody != null) req.setBody(textBody);
        if (blobBody != null) req.setBodyAsBlob(blobBody);
        for (String key : headers.keySet()) {
            req.setHeader(key, headers.get(key));
        }
        try {
            HttpResponse res = http.send(req);
            if (res != null && res.getStatusCode() == 200 || res.getStatusCode() == 201) 
            {
               return res.getBody();
              
            }
          
        } catch (Exception e) {
            System.debug('Callout exception: ' + e.getMessage());
        }

        return null;
    }

    private static String calculateCheckSum(Blob contentData) {
        if (contentData != null) {
            Blob hash = Crypto.generateDigest('SHA1', contentData);
            return EncodingUtil.convertToHex(hash);
        }
        return null;
    }    
}
