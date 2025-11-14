@IsTest
private class ODMSValidateFiles_Test {

    class MockHttpResponseGenerator implements HttpCalloutMock {
        public HTTPResponse respond(HTTPRequest req) {
            HttpResponse res = new HttpResponse();
            res.setHeader('Content-Type', 'application/json');
            res.setBody('{"dmsId":"mockDms123"}');
            res.setStatusCode(200);
            return res;
        }
    }

    @isTest
    static void testUploadAllFilesToDMS() {

        Test.setMock(HttpCalloutMock.class, new MockHttpResponseGenerator());

        OContactRecording__c contRec = new OContactRecording__c(
            Name = 'Test Agreement',
            Vertical__c='HL'
        );
        insert contRec;

        ContentVersion cv = new ContentVersion(
            Title = 'Test File',
            PathOnClient = 'TestDoc.pdf',
            VersionData = Blob.valueOf('This is a test file.'),
            IsMajorVersion = true
        );
        insert cv;

        Id contentDocId = [
            SELECT ContentDocumentId ,ContentBodyId
            FROM ContentVersion 
            WHERE Id = :cv.Id
        ].ContentDocumentId;

        ContentDocumentLink cdl = new ContentDocumentLink(
            ContentDocumentId = contentDocId,
            LinkedEntityId = contRec.Id,
            ShareType = 'V',
            Visibility = 'AllUsers'
        );
        insert cdl;

        // ⬇️ IMPORTANT: Re-query before passing to your method
        ContentDocumentLink cdlFull = [
            SELECT 
        Id,
        ContentDocumentId,
        LinkedEntityId,
        LinkedEntity.Name,
        ContentDocument.Title,
        ContentDocument.FileType,
        ContentDocument.LatestPublishedVersion.Id,
        ContentDocument.LatestPublishedVersion.PathOnClient,
        ContentDocument.LatestPublishedVersion.FileExtension,
        ContentDocument.LatestPublishedVersion.VersionData,
        ContentDocument.LatestPublishedVersion.ContentSize,
        ContentDocument.LatestPublishedVersion.ContentBodyId,
        ContentDocument.LatestPublishedVersion.CreatedById,
        ContentDocument.LatestPublishedVersion.CreatedBy.Name,
        ContentDocument.LatestPublishedVersion.CreatedDate
    FROM ContentDocumentLink
            WHERE Id = :cdl.Id
        ];

        Test.startTest();
        ODMSValidateFiles.uploadSingleFile(cdlFull);
        Test.stopTest();

        System.debug('LinkedEntity : ' + cdlFull.LinkedEntity.Name);
        System.debug('ContentDocumentId : ' + cdlFull.ContentDocumentId);

        // Final queries (optional)
        List<ODMS_File__c> dmsFiles = [
            SELECT Id, ContentDocumentId__c, DMS_Url__c 
            FROM ODMS_File__c
        ];

        OContactRecording__c updatedContRec = [
            SELECT Id,Name 
            FROM OContactRecording__c 
            WHERE Id = :contRec.Id
        ];
    }
    
 @IsTest
static void testUpdateFlagIfAllowed() {
    OContactRecording__c testRecord = new OContactRecording__c(
        Vertical__c = 'HL'
    );
    insert testRecord;
    testRecord = [SELECT Id, Name FROM OContactRecording__c WHERE Id = :testRecord.Id];
    //System.assertEquals(false, testRecord.Ready_For_DMS_Upload__c);

    ODMSValidateFiles.updateFlagIfAllowed(testRecord.Id);

    testRecord = [SELECT Id, Name FROM OContactRecording__c WHERE Id = :testRecord.Id];
    //System.assertEquals(true, testRecord.Ready_For_DMS_Upload__c);

    Account acc = new Account(Name='Test Account');
    insert acc;

    ODMSValidateFiles.updateFlagIfAllowed(acc.Id);
    acc = [SELECT Id FROM Account WHERE Id = :acc.Id]; // No Ready_For_DMS_Upload__c field
    //System.assertNotEquals(true, acc.get('Ready_For_DMS_Upload__c')); // safe check
}

}
