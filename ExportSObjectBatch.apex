public with sharing class ExportSObjectBatch implements Database.Batchable<SObject>, Database.Stateful {
    private String sObjectType;
    private Date startDate;
    private Date endDate;
    private Id resultRecordId;
    private List<Map<String, Object>> accumulatedData = new List<Map<String, Object>>();
    private List<String> jsonChunks = new List<String>();
    public String query;

    public ExportSObjectBatch(String sObjectType, Date startDate, Date endDate) {
        this.sObjectType = sObjectType;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public Database.QueryLocator start(Database.BatchableContext bc) {
        // Create result tracking record
        Export_Result__c res = new Export_Result__c(
            Job_Id__c = bc.getJobId(),
            Status__c = 'In Progress',
            ObjectName__c = sObjectType,
            StartDate__c = startDate,
            EndDate__c = endDate
        );
        insert res;
        resultRecordId = res.Id;

        return Database.getQueryLocator(buildQuery());
    }

    // public void execute(Database.BatchableContext bc, List<SObject> scope) {
    //    try {
    //         // Convert only current chunk to JSON
    //         List<Map<String,Object>> chunkData = new List<Map<String,Object>>();
    //         for (SObject rec : scope) {
    //             chunkData.add((Map<String,Object>) JSON.deserializeUntyped(JSON.serialize(rec)));
    //         }
    //         jsonChunks.add(JSON.serialize(chunkData));
    //     } catch (Exception e) {
    //         System.debug('Error serializing chunk: ' + e.getMessage());
    //     }
    // }
    public void execute(Database.BatchableContext bc, List<SObject> scope) {
    try {
        for (SObject rec : scope) {
            accumulatedData.add((Map<String,Object>) JSON.deserializeUntyped(JSON.serialize(rec)));
        }
    } catch (Exception e) {
        System.debug('Error serializing chunk: ' + e.getMessage());
    }
}

    // public void finish(Database.BatchableContext bc) {
    //     try {
    //         String jsonData = JSON.serialize(accumulatedData);

    //         Export_Result__c res = [
    //             SELECT Id FROM Export_Result__c WHERE Id = :resultRecordId LIMIT 1
    //         ];
    //        // res.JsonData__c = jsonData;
    //         res.Status__c = 'Completed';
    //         update res;
    //     } catch (Exception e) {
    //         Export_Result__c res = [
    //             SELECT Id FROM Export_Result__c WHERE Id = :resultRecordId LIMIT 1
    //         ];
    //         res.Status__c = 'Failed: ' + e.getMessage();
    //         update res;
    //     }
    // }
    public void finish(Database.BatchableContext bc) {
    try {
        String jsonData = JSON.serialize(accumulatedData);

        // Store in session cache with job ID as key
       // Cache.Session.put(bc.getJobId(), jsonData);
        Cache.Org.put(bc.getJobId(), jsonData);

        Export_Result__c res = [
            SELECT Id FROM Export_Result__c WHERE Id = :resultRecordId LIMIT 1
        ];
        res.Status__c = 'Completed';
        update res;
    } catch (Exception e) {
        Export_Result__c res = [
            SELECT Id FROM Export_Result__c WHERE Id = :resultRecordId LIMIT 1
        ];
        res.Status__c = 'Failed: ';
        update res;
    }
}


    // Helper method to retrieve JSON after batch finishes
    public static String getJsonByJobId(Id jobId) {
       // return (String) Cache.Session.get(jobId);
       return (String) Cache.Org.get(jobId);
    }


   private String buildQuery() {
    System.debug('buildQuery calling...');
    System.debug('Selected Object Name : '+this.sObjectType);
         // ===  OContactRecording ===    

        if (sObjectType == 'OContactRecording__c') {

            query= 'SELECT Id, Name, ActionCode__c, ActionCode__r.Name, ' +

                   'Agreement__r.Name, Agreement__r.Customer_Name__c, ' +

                   'Agreement__r.Status__c, Agreement__r.NPASTAGE__c, ' +

                   'LastModifiedBy.Name,' +

                   'Question1__c, Response1__c, Question2__c, Response2__c, ' +

                   'Question3__c, Response3__c, Question4__c, Response4__c, ' +

                   'Question5__c, Response5__c ' +

                   'FROM ' + sObjectType +

                   ' WHERE Agreement__r.Status__c = \'Closed\' ' +

                   'AND Agreement__r.NPASTAGE__c = \'REGULAR\' ' +

                   'AND CreatedDate >= :startDate AND CreatedDate <= :endDate';

        }

        // ===  OReceipt Query ===

        else if (sObjectType == 'OReceipt__c') {

           query=

                'SELECT Id, Name, AgreementId__c, AgreementNo__c, AgreementNo__r.AgreementId__c, ' +

                'AgreementNumber__c, Amount__c, Approval_Ref_Number__c, ApprovalFlowType__c, Approver1__c, ' +

                'Approver1__r.EmployeeNumber, Approver1__r.LastName, Area__c, AutoAlloc__c, BankAccountNo__c, ' +

                'BankBranch__c, BankBranchID__c, BankID__c, BankName__c, BatchId__c, BillingCity__c, ' +

                'BillingCountry__c, BillingPostalCode__c, BillingState__c, BillingStreet__c, BMGroupId__c, ' +

                'BounceDate__c, Branch__c, Branch__r.BranchCode__c, BranchId__c, CholaBank__c, CIFId__c, ' +

                'ChallanNo__c, ChallenId__c, ChequeId__c, Customer__c, CustomerName__c, OwnerEmpNo__c, ' +

                'IFSCCode__c, CustomerCIFId__c, OwnerDesignation__c, OwnerBranch__c, Description__c, ' +

                'DraftAmount__c, LMSChequeNo__c, LMSPaymentMode__c, CreatedDate ' +

                'FROM ' + sObjectType +

                ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate';

        }

        // === Approval Request ===

        else if (sObjectType == 'OApprovalRequest__c') {

            query=

                'SELECT Id, Name, AFC__c, Agreement__c, Agreement__r.AgreementNo__c, Agreement_number__c, ' +

                'AllocatedCFEId__c, AlreadyRaisedReceipt__c, Amount__c, Approval_1_Emp_Number__c, ApprovalDate__c, ' +

                'ApprovalExpiry__c, ApprovalFlowType__c, ApprovalRuleName__c, ApprovalType__c, Approved_By__c, ' +

                'ApprovedBySecondaryApproval__c, ApprovedByUserId__c, Approver1__c, Approver1__r.EmployeeNumber, ' +

                'AreaName__c, Branch__c, Branch_Name__c, BT_Banker__c, CBC__c, Closure_Reason__c, Closure_Type__c, ' +

                'CollectionAmount__c, CollectionType__c, CreatedById, CreatedDate, ' +

                'CrossSellCharges__c, CurrentRecommenderApprover__c, Customer_Profile_Previous_Auction__c, ' +

                'CustomerBackground__c, CustomerName__c, CustomerNameText__c, DisbursedAmount__c, DMS_Ids__c, ' +

                'DMSFile_Id__c, DPD__c, DRT_Amount__c, EditableFieldName__c, EMI__c, EmpowermentApprovalStatus__c, ' +

                'FC_Excess_Amount__c, Fee_of_Legal_Action__c, Foreclosure_Waiver_Amount__c, Legal_Case_Agreement_No__c, ' +

                'Legal_Case_Hold_Reason__c, Legal_Case_New_LookUp__c, Legal_Expense_Invoice_Amount_New_Value__c, ' +

                'Legal_Expense_Invoice_Amount_Old_Value__c, OtherAmount__c, PaymentDetails__c, PaymentMethod__c, ' +

                'PendingShortfall__c, ProductCode__c, Status__c, Stage__c, Initiator_EmpID__c, Receipt__c, ' +

                'ReceiptBranch__c, Total_Charge_Due__c, Total_Charge_Waiver__c, Vertical__c ' +

                'FROM ' + sObjectType +

                ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate';

        }

        // === Challan ===

        else if (sObjectType == 'OChallan__c') {

            query=

                'SELECT Id, Name, Amount__c, ApprovalFlowType__c, Approver1__c, BankDepositedBatchesIncluded__c, ' +

                'BankName__c, Branch__c, Challan_Overdue__c, ChallanDueDate__c, ChallanUploadDate__c, Challened_Date__c, ' +

                'CholasACNumber__c, CreatedBy.EmployeeNumber, CreatedDate, DelayReason__c, EmpowermentApprovalStatus__c, ' +

                'Entity__c, ExternalKey__c, ExternalSystemDate__c, IsEscalated__c, LastActivityDate, LastModifiedDate, ' +

                'MyBranch__c, MyChallan__c, NumberOfReceipts__c, OwnerBranch__c, Owner.Name, OwnerDesignation__c, OwnerEmpNo__c, ' +

                'OwnerId, PaymentMode__c, PhysicalChallanDate__c, PhysicalChallanNo__c, ProductCode__c, ReasonforEscalation__c, ' +

                'Recommender1__c, RecordType.DeveloperName, Remarks__c, Source__c, Stage__c, Status__c, Teller__c, ' +

                'TotalNumberofDaysDelayed__c, Type__c, UserGroupName__c, Vertical__c ' +

                'FROM ' + sObjectType +

                ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate ' ;

        }

        else if (sObjectType == 'OReceiptBatch__c') {

   query=

        'SELECT Id, Name, BankDepositedBatch__c, Challan__c, CreatedBy.EmployeeNumber, CreatedDate, ExternalKey__c, ' +

        'HandOffApprovedDate__c, HandOffDate__c, HandOffStatus__c, HandOffTo__c, HandOffToBranch__c, HandsOffToUserDetail__c, ' +

        'LastActivityDate, Owner.Name, OwnerBranch__c, OwnerDesignation__c, OwnerEmpNo__c, PaymentMode__c, ProductCode__c, ' +

        'RecordType.DeveloperName, Source__c, Status__c, TotalAmount__c, TotalReceipts__c, TransactionId__c, Type__c, ' +

        'UserBranch__c, UserBranch__r.BatchId__c, UserGroupName__c, Vertical__c ' +

        'FROM ' + sObjectType +

        ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate';

}

        return query;

    }
 
 
}
