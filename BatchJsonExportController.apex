public with sharing class BatchJsonExportController {
    @AuraEnabled
    public static String getBulkData(String sObjectType, Date startDate, Date endDate) {
        System.debug('Selected sObject Name : ' + sObjectType);
        System.debug('Selected Start Date : ' + startDate);
        System.debug('Selected End Date : ' + endDate);
        Id jobId = Database.executeBatch(new ExportSObjectBatch(sObjectType, startDate, endDate), 500);
        return jobId;
     
    }

    @AuraEnabled
    public static String getBatchJobStatus(Id jobId) {
        AsyncApexJob job = [SELECT Status FROM AsyncApexJob WHERE Id = :jobId LIMIT 1];
        return job.Status;
    }

    @AuraEnabled
    public static String getBatchJsonResult(Id jobId) {
        Export_Result__c res = [
            SELECT  JsonData__c,Status__c 
            FROM Export_Result__c 
            WHERE Job_Id__c = :jobId 
            LIMIT 1
        ]; 
        if (res.Status__c == 'Completed') {
            return res.JsonData__c;
        } else {
            throw new AuraHandledException('Export not completed yet or failed.');
        }
       
    }
    @AuraEnabled
    public static String downloadJson(Id jobId) {
        ExportSObjectBatch batch = new ExportSObjectBatch('', Date.today(), Date.today());
        String jsonData = ExportSObjectBatch.getJsonByJobId(jobId);
        if (String.isBlank(jsonData)) {
            throw new AuraHandledException('JSON not ready yet.');
        }
        return jsonData;
    }
    // @AuraEnabled
    // public static String downloadJson(Id jobId) {
    // String json = (String) Cache.Session.get(jobId);
    // if (String.isBlank(json)) {
    //     throw new AuraHandledException('JSON not ready yet.');
    // }
    // return json;
//}

}
