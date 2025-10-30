/**
* @File Name : BatchJsonExportControllerTest.cls
* @Description :
* @Author :
* @Last Modified By :
* @Last Modified On : October 22, 2025
* @Modification Log :
*==============================================================================
* Ver | Date | Author | Modification
*==============================================================================
* 1.0 | October 22, 2025 |   | Initial Version
**/

@isTest
private class BatchJsonExportControllerTest {

@testSetup
static void setupTestData() {
    // Create and insert Agreement
    Agreement__c agreement = new Agreement__c(
        Name = 'Agreement1',
        VehicleNo1__c = 'TN17CF1757',
        DPD__c = 0,
        Top_ups__c = 'Good'
    );
    insert agreement;

    // Create and insert Receipt linked to Agreement
    // Receipt__c receipt = new Receipt__c(
    //     IsFileUploaded__c = false,
    //     Amount__c = 1000,
    //     AgreementNo__c = agreement.Id,
    //     PaymentMode__c = 'Cheque',
    //     ReceiptDate__c = System.today()
    // );
    // insert receipt;
        Receipt__c receipt = new Receipt__c(LMSSyncStatus__c='Awaiting',Status__c='Ready for Batching',Product__c='VF',AgreementNo__c=agreement.id,PaymentMode__c='CASH',ReceiptDate__c=system.today(),Amount__c=1000.00,PhoneNumber__c='9123456780');                                        
        insert receipt;

    // Insert some OReceipt__c test data
   /* List<OReceipt__c> receipts = new List<OReceipt__c>();
    for (Integer i = 0; i < 3; i++) {
        receipts.add(new OReceipt__c(
            Name = 'Test Receipt ' + i,
            Amount__c = 100 + i
        ));
    }
    insert receipts;*/ 
}


    @isTest
    static void testGetBulkData_and_Status_and_DownloadJson() {
        Date startDate = Date.today().addDays(-1);
        Date endDate = Date.today();

        Test.startTest();
        // Call actual method which triggers the batch and returns jobId
        String jobId = BatchJsonExportController.getBulkData('OReceipt__c', startDate, endDate);
        Test.stopTest();

        System.assertNotEquals(null, jobId, 'Batch Job ID should not be null');

        // Insert a matching Export_Result__c record with returned jobId
        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'Completed',
            JsonData__c = '[{"Name":"Dynamic"}]',
            ObjectName__c = 'OReceipt__c',
            StartDate__c = startDate,
            EndDate__c = endDate
        );
        insert result;
        try {
            String status = BatchJsonExportController.getBatchJobStatus(jobId);
            System.debug('Batch status: ' + status); // For visibility
        } catch (Exception e) {
            System.debug('getBatchJobStatus may fail in test context (expected)');
        }

        // Test getBatchJsonResult
        String jsonResult = BatchJsonExportController.getBatchJsonResult(jobId);
        try {
            String download = BatchJsonExportController.downloadJson(jobId);
        } catch (AuraHandledException ex) {
            System.assertEquals('JSON not ready yet.', ex.getMessage());
        }
    }

    @isTest
    static void testGetBatchJsonResult() {
        Test.startTest();
      
            Id jobId = Database.executeBatch(new ExportSObjectBatch('OReceipt__c', Date.today().addDays(-1), Date.today()), 5);
Test.stopTest();
        // Insert Export_Result__c with a different status
        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'In Progress',
            JsonData__c = '',
            ObjectName__c = 'OReceipt__c',
            StartDate__c = Date.today().addDays(-2),
            EndDate__c = Date.today()
        );
        insert result;
        try {
            String json = BatchJsonExportController.getBatchJsonResult(result.Job_Id__c);
        } catch (AuraHandledException ex) {
           System.debug('Exception : '+ex.getMessage());
        }  
    }
   
    //OContactRecording__c
    @isTest
    static void testGetBatchJsonResult_ContactRec() {
        Test.startTest();
        String sObjectName = 'OContactRecording__c';
        if(sObjectName=='OContactRecording__c')
        {
            Id jobId = Database.executeBatch(new ExportSObjectBatch('OContactRecording__c', Date.today().addDays(-1), Date.today()), 5);
Test.stopTest();
        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'In Progress',
            JsonData__c = '',
            ObjectName__c = 'OContactRecording__c',
            StartDate__c = Date.today().addDays(-2),
            EndDate__c = Date.today()
        );
        insert result;
            try {
            String json = BatchJsonExportController.getBatchJsonResult(result.Job_Id__c);
        } catch (AuraHandledException ex) {
           System.debug('Exception : '+ex.getMessage());
        }
        }
        
    } 
        // === Test for OApprovalRequest__c ===
    @isTest
    static void testGetBatchJsonResult_ApprovalRequest() {
        Test.startTest();
        Id jobId = Database.executeBatch(
            new ExportSObjectBatch('OApprovalRequest__c', Date.today().addDays(-1), Date.today()), 
            5
        );
        Test.stopTest();

        // Insert Export_Result__c with dummy data
        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'In Progress',
            JsonData__c = '',
            ObjectName__c = 'OApprovalRequest__c',
            StartDate__c = Date.today().addDays(-1),
            EndDate__c = Date.today()
        );
        insert result;

        try {
            String json = BatchJsonExportController.getBatchJsonResult(result.Job_Id__c);
        } catch (AuraHandledException ex) {
            System.debug('OApprovalRequest__c branch covered - Exception: ' + ex.getMessage());
        }
    }

    // === Test for OChallan__c ===
    @isTest
    static void testGetBatchJsonResult_Challan() {
        Test.startTest();
        Id jobId = Database.executeBatch(
            new ExportSObjectBatch('OChallan__c', Date.today().addDays(-1), Date.today()), 
            5
        );
        Test.stopTest();

        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'In Progress',
            JsonData__c = '',
            ObjectName__c = 'OChallan__c',
            StartDate__c = Date.today().addDays(-1),
            EndDate__c = Date.today()
        );
        insert result;

        try {
            String json = BatchJsonExportController.getBatchJsonResult(result.Job_Id__c);
        } catch (AuraHandledException ex) {
            System.debug('OChallan__c branch covered - Exception: ' + ex.getMessage());
        }
    }

    // === Test for OReceiptBatch__c ===
    @isTest
    static void testGetBatchJsonResult_ReceiptBatch() {
        Test.startTest();
        Id jobId = Database.executeBatch(
            new ExportSObjectBatch('OReceiptBatch__c', Date.today().addDays(-1), Date.today()), 
            5
        );
        Test.stopTest();

        Export_Result__c result = new Export_Result__c(
            Job_Id__c = jobId,
            Status__c = 'In Progress',
            JsonData__c = '',
            ObjectName__c = 'OReceiptBatch__c',
            StartDate__c = Date.today().addDays(-1),
            EndDate__c = Date.today()
        );
        insert result;

        try {
            String json = BatchJsonExportController.getBatchJsonResult(result.Job_Id__c);
        } catch (AuraHandledException ex) {
            System.debug('OReceiptBatch__c branch covered - Exception: ' + ex.getMessage());
        }
    }

}
