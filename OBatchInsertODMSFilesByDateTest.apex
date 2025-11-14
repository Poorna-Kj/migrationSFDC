/**
* @File Name : OBatchInsertODMSFilesByDateTest.cls
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
@IsTest
public class OBatchInsertODMSFilesByDateTest {

    // Mock for HTTP callout in ODMSValidateFiles.uploadSingleFile
    class MockHttpResponseGenerator implements HttpCalloutMock {
        public HTTPResponse respond(HTTPRequest req) {
            HttpResponse res = new HttpResponse();
            res.setHeader('Content-Type', 'application/json');
            res.setBody('{"dmsId":"mockDms123"}');
            res.setStatusCode(200);
            return res;
        }
    }

    // Utility method to create a ContentVersion + ContentDocumentLink for a parent record
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

        // Create parent with Vertical__c
        OContactRecording__c recording = new OContactRecording__c(
            Name = 'Test Recording',
            Vertical__c = 'HL'
        );
        insert recording;

        // Link ContentDocument
        ContentDocumentLink cdl = createContentDocLink(recording);

        // Insert duplicate file to cover "alreadyUploaded" skip
        ODMS_File__c existingFile = new ODMS_File__c(
            ContentDocumentId__c = cdl.ContentDocumentId,
            Migrate_Status__c = 'Success'
        );
        insert existingFile;

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OContactRecording__c', 'HL');
        Database.executeBatch(batchJob, 1);
        Test.stopTest();
    }

    @IsTest
    static void testBatchForOChallan() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OChallan__c challan = new OChallan__c(
            Vertical__c = 'HL'
        );
        insert challan;

        createContentDocLink(challan);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OChallan__c', 'HL');
        Database.executeBatch(batchJob, 1);
        Test.stopTest();
    }

 
   /* @IsTest
    static void testBatchForOReceipt() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OAccount__c acc = new OAccount__c(Name='Tambaram HL', CIFID__c='12345');
        insert acc;

        OAgreement__c agreement = new OAgreement__c(
            DisbursedAmount__c=1000.00,
            IRR__c=10,
            DisbursedDate__c=System.today()-5,
            AgreementNo__c='AGR-123',
            Status__c='Active',
            NPASTAGE__c='Regular',
            Customer__c=acc.Id
        );
        insert agreement;

       OReceipt__c receipt = new OReceipt__c(
    LMSSyncStatus__c = 'Awaiting',
    Status__c = 'Ready for Batching',
    Product__c = 'VF',
    AgreementNo__c = agreement.Id,
    AgreementNumber__c = agreement.Name,      // IMPORTANT
    CustomerCIFId__c = acc.CIFID__c,
    //Customer__c = acc.Id,
    PaymentMode__c = 'CASH',
    ReceiptDate__c = System.today(),
    Amount__c = 1000.00,
    PhoneNumber__c = '9123456780',
    Vertical__c = 'HL',
    ReceiptType__c = 'OD – Overdue',              // REQUIRED
    Source__c = 'Browser',                   // REQUIRED
    RecordTypes__c = 'Overdue',        // REQUIRED (matches devname)
    PaymentStatus__c = 'CHEQUE PAYMENT AUTHOR'
);
insert receipt;


        createContentDocLink(receipt);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceipt__c', 'HL');
        Database.executeBatch(batchJob, 1);
        Test.stopTest();
    } */ 

    @IsTest
    static void testBatchForOApprovalRequest() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OAccount__c acc = new OAccount__c(Name='Tambaram VF', CIFID__c='9999');
        insert acc;

        OAgreement__c agreement = new OAgreement__c(
            DisbursedAmount__c=5000.00,
            IRR__c=12,
            DisbursedDate__c=System.today()-5,
            AgreementNo__c='AGR-9999',
            Status__c='Active',
            NPASTAGE__c='Regular',
            Customer__c=acc.Id
        );
        insert agreement;

        OApprovalRequest__c approval = new OApprovalRequest__c(
            Status__c='Approved',
            Agreement__c=agreement.Id,
            Vertical__c='HL'
        );
        insert approval;

        createContentDocLink(approval);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OApprovalRequest__c', 'HL');
        Database.executeBatch(batchJob, 1);
        Test.stopTest();
    }
    
    
    @IsTest
    static void testBatchForOReceiptBatch() {
        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OReceiptBatch__c recBatch = new OReceiptBatch__c(
            Vertical__c = 'HL'
        );
        insert recBatch;

        createContentDocLink(recBatch);

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);

        Test.startTest();
        OBatchInsertODMSFilesByDate batchJob = new OBatchInsertODMSFilesByDate(startDate, endDate, 'OReceiptBatch__c', 'HL');
        Database.executeBatch(batchJob, 1);
        Test.stopTest();
    }
}
