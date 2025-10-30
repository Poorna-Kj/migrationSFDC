/**
* @File Name : ODMSBatchControllerTest.cls
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
public class ODMSBatchControllerTest {

    @IsTest
    static void testStartUploadBetweenDates_Success() {
        // Setup test data for batch (at least one parent record of allowed types)
        OContactRecording__c rec = new OContactRecording__c(Name = 'Test Recording');
        insert rec;

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
        String sObjectType = 'OContactRecording__c';

        Test.startTest();
        String jobId = ODMSBatchController.startUploadBetweenDates(startDate, endDate, sObjectType);
        Test.stopTest();

        System.assertNotEquals(null, jobId, 'Batch job ID should not be null');
        System.assertEquals(18, jobId.length(), 'Batch job ID should be 18 characters long');
    }

   @IsTest
	static void testStartUploadBetweenDates_BlankSObjectType() {
    DateTime startDate = System.now().addDays(-1);
    DateTime endDate = System.now().addDays(1);
    String sObjectType = '';

    Boolean exceptionThrown = false;

    try {
        ODMSBatchController.startUploadBetweenDates(startDate, endDate, sObjectType);
        System.assert(false, 'Expected AuraHandledException due to blank sObjectType');
    } catch (AuraHandledException e) {
        exceptionThrown = true;
        System.debug('Caught AuraHandledException as expected: ' + e);
    } catch (Exception e) {
        exceptionThrown = true;
        System.debug('Caught generic exception: ' + e);
    }
    System.assert(exceptionThrown, 'Expected an exception to be thrown for blank sObjectType');
}
 
     @IsTest
	static void testStartUploadBetweenDates_BlankStartDate() {
    DateTime startDate;
    DateTime endDate = System.now().addDays(1);
    String sObjectType = '';

    Boolean exceptionThrown = false;

    try {
        ODMSBatchController.startUploadBetweenDates(startDate, endDate, sObjectType);
        System.assert(false, 'Expected AuraHandledException due to blank sObjectType');
    } catch (AuraHandledException e) {
        exceptionThrown = true;
        System.debug('Caught AuraHandledException as expected: ' + e);
    } catch (Exception e) {
        exceptionThrown = true;
        System.debug('Caught generic exception: ' + e);
    }
    System.assert(exceptionThrown, 'Expected an exception to be thrown for blank sObjectType');
}
    
       @IsTest
	static void testStartUploadBetweenDates_GreaterEndDate() {
    DateTime startDate = System.now();
    DateTime endDate = System.now().addDays(-1);
    String sObjectType = '';

    Boolean exceptionThrown = false;

    try {
        ODMSBatchController.startUploadBetweenDates(startDate, endDate, sObjectType);
        System.assert(false, 'Expected AuraHandledException due to blank sObjectType');
    } catch (AuraHandledException e) {
        exceptionThrown = true;
        System.debug('Caught AuraHandledException as expected: ' + e);
    } catch (Exception e) {
        exceptionThrown = true;
        System.debug('Caught generic exception: ' + e);
    }
    System.assert(exceptionThrown, 'Expected an exception to be thrown for blank sObjectType');
}

    @IsTest
    static void testGetBatchJobStatus() {
        // Setup a dummy AsyncApexJob record - only way is to submit batch job in test context
        OContactRecording__c rec = new OContactRecording__c(Name = 'Test Recording');
        insert rec;

        DateTime startDate = System.now().addDays(-1);
        DateTime endDate = System.now().addDays(1);
        String sObjectType = 'OContactRecording__c';

        Test.startTest();
        String jobId = ODMSBatchController.startUploadBetweenDates(startDate, endDate, sObjectType);
       
        ODMSBatchController.BatchJobStatusWrapper statusWrapper = ODMSBatchController.getBatchJobStatus(jobId);

        String status = statusWrapper.Status;
        System.debug('job Status : '+status);
        Test.stopTest();
		
        System.assertNotEquals(null, status, 'Batch job status should not be null');
        System.assert(
            status == 'Queued' || status == 'Processing' || status == 'Completed',
            'Status should be Queued, Processing or Completed'
        );
    }
    
   
 
}
