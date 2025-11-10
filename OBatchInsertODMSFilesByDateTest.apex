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
* 1.0 | September 9, 2025 |   | Initial Version
**/


@IsTest
public class OBatchInsertODMSFilesByDateTest {

    class MockHttpResponseGenerator implements HttpCalloutMock {
        public HTTPResponse respond(HTTPRequest req) {
            HttpResponse res = new HttpResponse();
            res.setHeader('Content-Type', 'application/json');
            res.setBody('{"dmsId":"mockDms123"}');
            res.setStatusCode(200);
            return res;
        }
    }

    // Utility method to create a ContentVersion and ContentDocumentLink for a given sObject
    private static ContentDocumentLink createContentDocLink(SObject parentRecord) {
        ContentVersion cv = new ContentVersion(
            Title = 'Test File ' + parentRecord.Id,
            PathOnClient = 'TestDoc.pdf',
            VersionData = Blob.valueOf('This is a test file content'),
            IsMajorVersion = true
        );
        insert cv;

        // Get ContentDocumentId
        Id contentDocId = [SELECT ContentDocumentId FROM ContentVersion WHERE Id = :cv.Id].ContentDocumentId;

        ContentDocumentLink cdl = new ContentDocumentLink(
            ContentDocumentId = contentDocId,
            LinkedEntityId = parentRecord.Id,
            ShareType = 'V',
            Visibility = 'AllUsers'
        );
        insert cdl;

        return cdl;
    }

    @IsTest
    static void testBatchForOContactRecording() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OContactRecording__c record within batch date range
        OContactRecording__c recording = new OContactRecording__c(
            Name = 'Test Recording'
        );
        insert recording;

        // Link ContentDocument to OContactRecording__c
        createContentDocLink(recording);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OContactRecording__c');
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 
    
     @IsTest
    static void testBatchForOChallan() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OChallan__c record within batch date range
 		OChallan__c testChallan = new OChallan__c();
    	insert testChallan;

        // Link ContentDocument to OChallan__c
        createContentDocLink(testChallan);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OChallan__c');
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 
    
     @IsTest
    static void testBatchForOReceiptBatch() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OReceiptBatch__c record within batch date range
 		OReceiptBatch__c testChallan = new OReceiptBatch__c();
    	insert testChallan;

        // Link ContentDocument to OReceiptBatch__c
        createContentDocLink(testChallan);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceiptBatch__c');
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 

    /*@IsTest
    static void testBatchForOReceipt() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OReceipt__c record within batch date range
        OReceipt__c receipt = new OReceipt__c(
            ChequeId__c = 'Test123', Amount__c = 50000, Status__c = 'Ready for Batching'
        );
        insert receipt;
       
        // Link ContentDocument to OReceipt__c
        createContentDocLink(receipt);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceipt__c');
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } */ 

    @IsTest
    static void testBatchForOApprovalRequest() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());
        OAccount__c acc = new OAccount__c(Name='Tambaram VF',CIFID__c ='1234');
        insert acc;
        OAgreement__c agreement = new OAgreement__c(DisbursedAmount__c=1000.00,IRR__c=10,DisbursedDate__c=System.today()-5,AgreementNo__c='12345',Status__c='Active',NPASTAGE__c='Regular',Customer__c=acc.id);
        insert agreement;
        // Create OApprovalRequest__c record within batch date range
        OApprovalRequest__c approvalRequest = new OApprovalRequest__c(
           Status__c = 'Approved',Agreement__c = agreement.Id
        );
        insert approvalRequest;

        // Link ContentDocument to OApprovalRequest__c
        createContentDocLink(approvalRequest);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OApprovalRequest__c');
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    }

}


##############################################################


Nov 10 - 2025

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
* 1.0 | September 9, 2025 |   | Initial Version
**/


@IsTest
public class OBatchInsertODMSFilesByDateTest {

    class MockHttpResponseGenerator implements HttpCalloutMock {
        public HTTPResponse respond(HTTPRequest req) {
            HttpResponse res = new HttpResponse();
            res.setHeader('Content-Type', 'application/json');
            res.setBody('{"dmsId":"mockDms123"}');
            res.setStatusCode(200);
            return res;
        }
    }

    // Utility method to create a ContentVersion and ContentDocumentLink for a given sObject
    private static ContentDocumentLink createContentDocLink(SObject parentRecord) {
        ContentVersion cv = new ContentVersion(
            Title = 'Test File ' + parentRecord.Id,
            PathOnClient = 'TestDoc.pdf',
            VersionData = Blob.valueOf('This is a test file content'),
            IsMajorVersion = true
        );
        insert cv;

        Id contentDocId = [SELECT ContentDocumentId FROM ContentVersion WHERE Id = :cv.Id].ContentDocumentId;

        ContentDocumentLink cdl = new ContentDocumentLink(
            ContentDocumentId = contentDocId,
            LinkedEntityId = parentRecord.Id,
            ShareType = 'V',
            Visibility = 'AllUsers'
        );
        insert cdl;

        return cdl;
    }

    @IsTest
    static void testBatchForOContactRecording() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OContactRecording__c record within batch date range
        OContactRecording__c recording = new OContactRecording__c(
            Name = 'Test Recording'
        );
        insert recording;

        // Link ContentDocument to OContactRecording__c
        createContentDocLink(recording);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
		String vertical ='HL';
        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OContactRecording__c',vertical);
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 
    
     @IsTest
    static void testBatchForOChallan() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OChallan__c record within batch date range
 		OChallan__c testChallan = new OChallan__c();
    	insert testChallan;

        // Link ContentDocument to OChallan__c
        createContentDocLink(testChallan);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
		String vertical ='HL';
        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OChallan__c',vertical);
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 
    
     @IsTest
    static void testBatchForOReceiptBatch() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        // Create OReceiptBatch__c record within batch date range
 		OReceiptBatch__c testReceiptBatch = new OReceiptBatch__c();
    	insert testReceiptBatch;

        // Link ContentDocument to OReceiptBatch__c
        createContentDocLink(testReceiptBatch);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
		String vertical ='HL';
        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceiptBatch__c',vertical);
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 
    
         @IsTest
    static void testBatchForOReceipt() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OAccount__c acc = new OAccount__c(Name='Tambaram HL',CIFID__c ='12345');
        insert acc;
         OAgreement__c agreement = new OAgreement__c(DisbursedAmount__c=1000.00,IRR__c=10,DisbursedDate__c=System.today()-5,AgreementNo__c='12345',Status__c='Active',NPASTAGE__c='Regular',Customer__c=acc.id);
        insert agreement;
         Receipt__c receipt = new Receipt__c(LMSSyncStatus__c='Awaiting',Status__c='Ready for Batching',Product__c='VF',AgreementNo__c=agreement.Name,PaymentMode__c='CASH',ReceiptDate__c=system.today(),Amount__c=1000.00,PhoneNumber__c='9123456780');                                        
        insert receipt;

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
		String vertical ='HL';
        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceipt__c',vertical);
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    } 

 
    @IsTest
    static void testBatchForOApprovalRequest() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());
         OAccount__c acc = new OAccount__c(Name='Tambaram VF',CIFID__c ='1234');
        insert acc;
         OAgreement__c agreement = new OAgreement__c(DisbursedAmount__c=1000.00,IRR__c=10,DisbursedDate__c=System.today()-5,AgreementNo__c='12345',Status__c='Active',NPASTAGE__c='Regular',Customer__c=acc.id);
        insert agreement;
         Receipt__c receipt = new Receipt__c(LMSSyncStatus__c='Awaiting',Status__c='Ready for Batching',Product__c='VF',AgreementNo__c=agreement.Name,PaymentMode__c='CASH',ReceiptDate__c=system.today(),Amount__c=1000.00,PhoneNumber__c='9123456780');                                        
        insert receipt;
       
        // Create OApprovalRequest__c record within batch date range
        OApprovalRequest__c approvalRequest = new OApprovalRequest__c(
           Status__c = 'Approved',Agreement__c = agreement.Id
        );
        insert approvalRequest;

        // Link ContentDocument to OApprovalRequest__c
        createContentDocLink(approvalRequest);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
		String vertical ='HL';
        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OApprovalRequest__c',vertical);
        Database.executeBatch(batchJob, 200);
        Test.stopTest();
    }

}


#############################################################
