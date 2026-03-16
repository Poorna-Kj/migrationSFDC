public class DMSFileJSONFormatterBatch implements Database.Batchable<SObject> {

    public Database.QueryLocator start(Database.BatchableContext bc) {
        return Database.getQueryLocator(
            'SELECT Id, DMS_JSON__c, JSON_Data_Format__c FROM DMS_File__c WHERE DMS_JSON__c != null'
        );
    }

    public void execute(Database.BatchableContext bc, List<DMS_File__c> scope) {

        System.debug('Batch size: ' + scope.size());

        List<DMS_File__c> recordsToUpdate = new List<DMS_File__c>();

        for (DMS_File__c rec : scope) {

            String rawJson = rec.DMS_JSON__c;

            if (rawJson != null) {

                String cleanJson = rawJson.replace('&quot;', '"')
                                          .replace('&amp;', '&')
                                          .replace('&lt;', '<')
                                          .replace('&gt;', '>');

                if(rec.JSON_Data_Format__c != cleanJson){
                    rec.JSON_Data_Format__c = cleanJson;
                    recordsToUpdate.add(rec);
                }
            }
        }

        System.debug('Records to update: ' + recordsToUpdate.size());

        if(!recordsToUpdate.isEmpty()){
            update recordsToUpdate;
        }
    }

    public void finish(Database.BatchableContext bc) {
        System.debug('Batch Job Finished');
    }
}
