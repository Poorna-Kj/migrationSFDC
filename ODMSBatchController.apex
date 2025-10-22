/**
* @File Name : ODMSBatchController.cls
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

public class ODMSBatchController {
	
    @AuraEnabled
public static String startUploadBetweenDates(DateTime startDate, DateTime endDate, String sObjectType) {
    if (startDate == null || endDate == null) {
        throw new AuraHandledException('Start and End date are required.');
    }

    if (endDate < startDate) {
        throw new AuraHandledException('End date must be after Start date.');
    }
     if (String.isBlank(sObjectType)) {
            throw new AuraHandledException('sObject type is required.');
        }

    Id jobId = Database.executeBatch(new OBatchInsertODMSFilesByDate(startDate, endDate, sObjectType), 1);
    return (String)jobId;
}

@AuraEnabled
public static String getBatchJobStatus(String jobId) {
    AsyncApexJob job = [
        SELECT Id, Status, NumberOfErrors, JobItemsProcessed, TotalJobItems
        FROM AsyncApexJob
        WHERE Id = :jobId
        LIMIT 1
    ];

    return job.Status;
}


}
