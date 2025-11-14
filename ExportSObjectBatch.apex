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
    public void execute(Database.BatchableContext bc, List<SObject> scope) {
    try {
        for (SObject rec : scope) {
            accumulatedData.add((Map<String,Object>) JSON.deserializeUntyped(JSON.serialize(rec)));
        }
    } catch (Exception e) {
        System.debug('Error serializing chunk: ' + e.getMessage());
    }
}
    public void finish(Database.BatchableContext bc) {
    try {
        String jsonData = JSON.serialize(accumulatedData);
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

                   'LastModifiedBy.Name,Vertical__c,' +

                   'Question1__c, Response1__c, Question2__c, Response2__c, ' +

                   'Question3__c, Response3__c, Question4__c, Response4__c, ' +

                   'Question5__c, Response5__c, ' +
                   'Question6__c, Response6__c, ' +
            'Question7__c, Response7__c, Question8__c, Response8__c, Question9__c, Response9__c, ' +
            'Question10__c, Response10__c, Question11__c, Response11__c, Question12__c, Response12__c, ' +
            'Question13__c, Response13__c, Question14__c, Response14__c, Question15__c, Response15__c, ' +
            'Question16__c, Response16__c, Question17__c, Response17__c, Question18__c, Response18__c, ' +
            'Question19__c, Response19__c, Question20__c, Response20__c, Question21__c, Response21__c, ' +
            'Question22__c, Response22__c, Question23__c, Response23__c, Question24__c, Response24__c, ' +
            'Question25__c, Response25__c, Question26__c, Response26__c, Question27__c, Response27__c, ' +
            'Question28__c, Response28__c, Question29__c, Response29__c, Question30__c, Response30__c, ' +
            'Question31__c, Response31__c, Question32__c, Response32__c, Question33__c, Response33__c, ' +
            'Question34__c, Response34__c, Question35__c, Response35__c, Question36__c, Response36__c, ' +
            'Question37__c, Response37__c, Question38__c, Response38__c, Question39__c, Response39__c, ' +
            'Question40__c, Response40__c, Question41__c, Response41__c, Question42__c, Response42__c, ' +
            'Question43__c, Response43__c, Question44__c, Response44__c, Question45__c, Response45__c ' +

                   ' FROM ' + sObjectType +

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

                'DraftAmount__c, LMSChequeNo__c, LMSPaymentMode__c, CreatedDate ,Vertical__c' +

                ' FROM ' + sObjectType +

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

                ' FROM ' + sObjectType +

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

                ' FROM ' + sObjectType +

                ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate ' ;

        }

        else if (sObjectType == 'OReceiptBatch__c') {

   query=

        'SELECT Id, Name, BankDepositedBatch__c, Challan__c, CreatedBy.EmployeeNumber, CreatedDate, ExternalKey__c, ' +

        'HandOffApprovedDate__c, HandOffDate__c, HandOffStatus__c, HandOffTo__c, HandOffToBranch__c, HandsOffToUserDetail__c, ' +

        'LastActivityDate, Owner.Name, OwnerBranch__c, OwnerDesignation__c, OwnerEmpNo__c, PaymentMode__c, ProductCode__c, ' +

        'RecordType.DeveloperName, Source__c, Status__c, TotalAmount__c, TotalReceipts__c, TransactionId__c, Type__c, ' +

        'UserBranch__c, UserBranch__r.BatchId__c, UserGroupName__c, Vertical__c ' +

        ' FROM ' + sObjectType +

        ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate';

}
else if (sObjectType == 'OCollectionPayment__c') {

    query = 'SELECT Id, Name, AdviceAmount__c, AgreementNo__c, BatchId__c, ChargeAmount__c, ' +
            'ChargeId__c, ChargeName__c, ChargeTaxMaster__c, Cheque_Upload_Deadline_in_Days__c, ' +
            'ChequeId__c, ClosedAgreementNo__c, CollectionExternalKey__c, CollectionType__c, ' +
            'ExtractionDate__c, Included_for_Batching__c, Max_Amount_Allowed_in_Cash_Mode__c, ' +
            'Max_Amount_CFE_Can_Carry__c, Max_Amount_Payable_for_Non_Agreement__c, ' +
            'Max_No_of_Batches_Open_Per_CFE__c, Max_No_of_Cash_Receipts_Open_per_CFE__c, ' +
            'Max_No_of_Cash_Receipts_Un_Batched__c, Max_No_of_Days_Cheque_Can_Be_Backdated__c, ' +
            'Max_No_of_Days_POS_Can_Be_Backdated__c, Max_No_Of_Days_UnChallaned_Cash_Receipt__c, ' +
            'Max_No_of_Un_Challaned_Batches_Per_User__c, Max_Shortfall_Amount_per_Agreement__c, ' +
            'Min_Amount_For_PAN_Verification__c, Payment_Mode_Lap__c, PaymentId__c, Receipt__c, ' +
            'RelatedReceipts__c, Vertical__c, WaiverAmount__c, LastModifiedDate, LastActivityDate,CreatedDate ' +
            ' FROM ' + sObjectType +
            ' WHERE CreatedDate >= :startDate AND CreatedDate <= :endDate';
}

        return query;

    }
 
 
}
