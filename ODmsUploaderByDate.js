import { LightningElement, track } from 'lwc';
import startUploadBetweenDates from '@salesforce/apex/ODMSBatchController.startUploadBetweenDates';
import getBatchJobStatusDMS from '@salesforce/apex/ODMSBatchController.getBatchJobStatus';

import getJsonBatchStatus from '@salesforce/apex/BatchJsonExportController.getBatchJobStatus';
import getBulkData from '@salesforce/apex/BatchJsonExportController.getBulkData';
import downloadJson from '@salesforce/apex/BatchJsonExportController.downloadJson';

export default class ODmsUploaderByDate extends LightningElement {
    // ===== DMS Upload Section =====
    @track dmsStartDate;
    @track dmsEndDate;
    @track dmsSObjectType;
    @track dmsShowSpinner = false;
    @track dmsMessage = '';
    dmsJobId;

    // ===== JSON Export Section =====
    @track jsonStartDate;
    @track jsonEndDate;
    @track jsonSObjectType;
    @track jsonShowSpinner = false;
    @track jsonMessage = '';
    jsonJobId;

    @track sObjectOptions = [
        { label: 'OContactRecording', value: 'OContactRecording__c' },
        { label: 'OReceipt', value: 'OReceipt__c' },
        { label: 'OApprovalRequest', value: 'OApprovalRequest__c' },
        { label: 'OChallan', value: 'OChallan__c' },
        { label: 'OBatch', value: 'OReceiptBatch__c' }
    ];

    get sObjectExpOptions() {
        return [
            { label: 'OContactRecording', value: 'OContactRecording__c' },
            { label: 'OReceipt', value: 'OReceipt__c' },
            { label: 'OApprovalRequest', value: 'OApprovalRequest__c' },
            { label: 'OChallan', value: 'OChallan__c' },
            { label: 'OBatch', value: 'OReceiptBatch__c' },
            { label: 'ODMSFile', value: 'ODMS_File__c'}
        ];
    }

    // ===== DMS Upload Handlers =====
    handleStartDateChange(event) {
        this.dmsStartDate = event.target.value;
    }

    handleEndDateChange(event) {
        this.dmsEndDate = event.target.value;
    }

    handleObjectChange(event) {
        this.dmsSObjectType = event.detail.value;
    }

    async startUpload() {
        this.dmsShowSpinner = true;
        this.dmsMessage = '';

        if (!this.dmsStartDate || !this.dmsEndDate) {
            this.dmsMessage = 'Please select both start and end dates.';
            this.dmsShowSpinner = false;
            return;
        }

        try {
            this.dmsJobId = await startUploadBetweenDates({
                startDate: this.dmsStartDate,
                endDate: this.dmsEndDate,
                sObjectType: this.dmsSObjectType
            });
            this.dmsMessage = 'DMS upload started. Please wait...';
            this.startPollingDMS();
        } catch (error) {
            this.dmsMessage = 'Error: ' + (error.body?.message || error.message);
            this.dmsShowSpinner = false;
        }
    }

    // startPollingDMS() {
    //         const interval = setInterval(async () => {
    //         const status = await getJsonBatchStatus({ jobId: this.jsonJobId });
    //         if (status === 'Completed') {
    //             clearInterval(interval);
    //             const jsonData = await downloadJson({ jobId: this.jsonJobId });
    //             this.downloadFile(jsonData, this.jsonSObjectType, this.jsonStartDate, this.jsonEndDate);
    //             this.jsonShowSpinner = false;
    //             this.jsonMessage = 'Download ready!';
    //         } else if (status === 'Failed' || status === 'Aborted') {
    //             clearInterval(interval);
    //             this.jsonShowSpinner = false;
    //             this.jsonMessage = `Batch failed: ${status}`;
    //         }
    //     }, 5000);
    // }
    startPollingDMS() {
    const interval = setInterval(async () => {
        try {
            const status = await getBatchJobStatusDMS({ jobId: this.dmsJobId });
            console.log('DMS Batch status:', status);

            if (status === 'Completed') {
                clearInterval(interval);
                this.dmsShowSpinner = false;
                this.dmsMessage = '✅ DMS file upload completed successfully.';
            } else if (status === 'Failed' || status === 'Aborted') {
                clearInterval(interval);
                this.dmsShowSpinner = false;
                this.dmsMessage = `❌ DMS upload failed: ${status}`;
            }
        } catch (error) {
            clearInterval(interval);
            this.dmsShowSpinner = false;
            this.dmsMessage = 'Error checking DMS batch: ' + (error.body?.message || error.message);
        }
    }, 5000);
}

     async checkBatchStatusDMS() {
        try {
            const status = await getBatchJobStatusDMS({ jobId: this.dmsJobId });
            if (status === 'Completed') {
                this.messageDMS = '✅ DMS file upload completed successfully.';
                this.showSpinnerDMS = false;
                clearInterval(this.dmsPolling);
            } else if (status === 'Failed' || status === 'Aborted') {
                this.messageDMS = '❌ DMS upload failed: ' + status;
                this.showSpinnerDMS = false;
                clearInterval(this.dmsPolling);
            }
        } catch (error) {
            this.messageDMS = 'Error checking DMS batch: ' + (error.body?.message || error.message);
            this.showSpinnerDMS = false;
            clearInterval(this.dmsPolling);
        }
    }


    // ===== JSON Export Handlers =====
    handlesObjStartDateChange(event) {
        this.jsonStartDate = event.target.value;
        console.log('Selected Start Dt : ',this.jsonStartDate);
    }

    handlesObjEndDateChange(event) {
        this.jsonEndDate = event.target.value;
        console.log('Selected End Dt : ',this.jsonEndDate);
    }

    handlesExpObject(event) {
        this.jsonSObjectType = event.detail.value;
        console.log('Selected sObject Name : ',this.jsonSObjectType);
    }

   async sObjstartUpload() {
    try {
        this.jsonShowSpinner = true;
        this.jsonMessage = 'Starting batch...';

        const result = await getBulkData({
            sObjectType: this.jsonSObjectType, 
            startDate: this.jsonStartDate,
            endDate: this.jsonEndDate
        });

        if (result)  //&& result.jobId
        {
            //this.jsonJobId = result.jobId;
            this.jsonJobId = result;
            console.log('JobId : ',this.jsonJobId);
            this.jsonMessage = 'Batch started. Waiting for completion...';

            const interval = setInterval(async () => {
                try {
                    const status = await getJsonBatchStatus({ jobId: this.jsonJobId });
                    console.log('Batch status:', status);

                    if (status === 'Completed') {
                        clearInterval(interval);
                        this.jsonMessage = 'Preparing download...';
                        const jsonData = await downloadJson({ jobId: this.jsonJobId });

                        if (jsonData) {
                            this.downloadFile(jsonData, this.jsonSObjectType, this.jsonStartDate, this.jsonEndDate);
                            this.jsonMessage = 'Download ready!';
                        } else {
                            this.jsonMessage = 'No data available for download.';
                        }

                        this.jsonShowSpinner = false;
                    } else if (status === 'Failed' || status === 'Aborted') {
                        clearInterval(interval);
                        this.jsonShowSpinner = false;
                        this.jsonMessage = `Batch failed: ${status}`;
                    }
                } catch (pollError) {
                    clearInterval(interval);
                    console.error('Error while polling batch status:', pollError);
                    this.jsonShowSpinner = false;
                    this.jsonMessage = 'Error checking batch status.';
                }
            }, 5000);
        } else {
            this.jsonShowSpinner = false;
            this.jsonMessage = 'Unable to start batch.';
        }
    } catch (error) {
        console.error('Error starting batch:', error);
        this.jsonShowSpinner = false;
        this.jsonMessage = 'Failed to start JSON export.';
    }
}

    downloadFile(jsonData, sObjectType, startDate, endDate) {
        const blob = new Blob([jsonData], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${sObjectType}_${startDate}_to_${endDate}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
}

*****************************************************************************************
    import { LightningElement, track } from 'lwc';
import startUploadBetweenDates from '@salesforce/apex/ODMSBatchController.startUploadBetweenDates';
import getBatchJobStatusDMS from '@salesforce/apex/ODMSBatchController.getBatchJobStatus';

import getJsonBatchStatus from '@salesforce/apex/BatchJsonExportController.getBatchJobStatus';
import getBulkData from '@salesforce/apex/BatchJsonExportController.getBulkData';
import downloadJson from '@salesforce/apex/BatchJsonExportController.downloadJson';

export default class ODmsUploaderByDate extends LightningElement {
    // ===== DMS Upload Section =====
    @track dmsStartDate;
    @track dmsEndDate;
    @track dmsSObjectType;
    @track dmsShowSpinner = false;
    @track dmsMessage = '';
    dmsJobId;

    // ===== JSON Export Section =====
    @track jsonStartDate;
    @track jsonEndDate;
    @track jsonSObjectType;
    @track jsonShowSpinner = false;
    @track jsonMessage = '';
    jsonJobId;

    @track sObjectOptions = [
        { label: 'OContactRecording', value: 'OContactRecording__c' },
        { label: 'OReceipt', value: 'OReceipt__c' },
        { label: 'OApprovalRequest', value: 'OApprovalRequest__c' },
        { label: 'OChallan', value: 'OChallan__c' },
        { label: 'OBatch', value: 'OReceiptBatch__c' }
    ];

    get sObjectExpOptions() {
        return [
            { label: 'OContactRecording', value: 'OContactRecording__c' },
            { label: 'OReceipt', value: 'OReceipt__c' },
            { label: 'OApprovalRequest', value: 'OApprovalRequest__c' },
            { label: 'OChallan', value: 'OChallan__c' },
            { label: 'OBatch', value: 'OReceiptBatch__c' },
            { label: 'ODMSFile', value: 'ODMS_File__c'}
        ];
    }

    // ===== DMS Upload Handlers =====
    handleStartDateChange(event) {
        this.dmsStartDate = event.target.value;
    }

    handleEndDateChange(event) {
        this.dmsEndDate = event.target.value;
    }

    handleObjectChange(event) {
        this.dmsSObjectType = event.detail.value;
        console.log('Selected sObject name : ',this.dmsSObjectType );
    }

    async startUpload() {
        this.dmsShowSpinner = true;
        this.dmsMessage = '';

        if (!this.dmsStartDate || !this.dmsEndDate) {
            this.dmsMessage = 'Please select both start and end dates.';
            this.dmsShowSpinner = false;
            return;
        }

        try {
            this.dmsJobId = await startUploadBetweenDates({
                startDate: this.dmsStartDate,
                endDate: this.dmsEndDate,
                sObjectType: this.dmsSObjectType
            });
            this.dmsMessage = 'DMS upload started. Please wait...';
            this.startPollingDMS();
        } catch (error) {
            this.dmsMessage = 'Error: ' + (error.body?.message || error.message);
            this.dmsShowSpinner = false;
        }
    }

    startPollingDMS() {
    const interval = setInterval(async () => {
        try {
            const result = await getBatchJobStatusDMS({ jobId: this.dmsJobId });
            const status = result.status;
            const extendedStatus = result.extendedStatus;
            console.log('DMS Batch status:', status);

            if (status === 'Completed') {
                clearInterval(interval);
                this.dmsShowSpinner = false;
                this.dmsMessage = '✅ DMS file upload completed successfully.';
            } else if (status === 'Failed' || status === 'Aborted') {
                clearInterval(interval);
                this.dmsShowSpinner = false;
                // this.dmsMessage = `❌ DMS upload failed: ${status}`;
                this.dmsMessage = `❌ DMS upload failed: ${extendedStatus || status}`;
            }
        } catch (error) {
            clearInterval(interval);
            this.dmsShowSpinner = false;
            this.dmsMessage = 'Error checking DMS batch: ' + (error.body?.message || error.message);
        }
    }, 5000);
}

     async checkBatchStatusDMS() {
        try {
            const status = await getBatchJobStatusDMS({ jobId: this.dmsJobId });
            
            if (status === 'Completed') {
                this.messageDMS = '✅ DMS file upload completed successfully.';
                this.showSpinnerDMS = false;
                clearInterval(this.dmsPolling);
            } else if (status === 'Failed' || status === 'Aborted') {
                this.messageDMS = '❌ DMS upload failed: ' + status;
                this.showSpinnerDMS = false;
                clearInterval(this.dmsPolling);
            }
        } catch (error) {
            this.messageDMS = 'Error checking DMS batch: ' + (error.body?.message || error.message);
            this.showSpinnerDMS = false;
            clearInterval(this.dmsPolling);
        }
    }


    // ===== JSON Export Handlers =====
    handlesObjStartDateChange(event) {
        this.jsonStartDate = event.target.value;
        console.log('Selected Start Dt : ',this.jsonStartDate);
    }

    handlesObjEndDateChange(event) {
        this.jsonEndDate = event.target.value;
        console.log('Selected End Dt : ',this.jsonEndDate);
    }

    handlesExpObject(event) {
        this.jsonSObjectType = event.detail.value;
        console.log('Selected sObject Name : ',this.jsonSObjectType);
    }

   async sObjstartUpload() {
    try {
        this.jsonShowSpinner = true;
        this.jsonMessage = 'Starting batch...';

        const result = await getBulkData({
            sObjectType: this.jsonSObjectType, 
            startDate: this.jsonStartDate,
            endDate: this.jsonEndDate
        });

        if (result)  //&& result.jobId
        {
            //this.jsonJobId = result.jobId;
            this.jsonJobId = result;
            console.log('JobId : ',this.jsonJobId);
            this.jsonMessage = 'Batch started. Waiting for completion...';

            const interval = setInterval(async () => {
                try {
                    const status = await getJsonBatchStatus({ jobId: this.jsonJobId });
                    console.log('Batch status:', status);

                    if (status === 'Completed') {
                        clearInterval(interval);
                        this.jsonMessage = 'Preparing download...';
                        const jsonData = await downloadJson({ jobId: this.jsonJobId });

                        if (jsonData) {
                            this.downloadFile(jsonData, this.jsonSObjectType, this.jsonStartDate, this.jsonEndDate);
                            this.jsonMessage = 'Download ready!';
                        } else {
                            this.jsonMessage = 'No data available for download.';
                        }

                        this.jsonShowSpinner = false;
                    } else if (status === 'Failed' || status === 'Aborted') {
                        clearInterval(interval);
                        this.jsonShowSpinner = false;
                        this.jsonMessage = `Batch failed: ${status}`;
                    }
                } catch (pollError) {
                    clearInterval(interval);
                    console.error('Error while polling batch status:', pollError);
                    this.jsonShowSpinner = false;
                    this.jsonMessage = 'Error checking batch status.';
                }
            }, 5000);
        } else {
            this.jsonShowSpinner = false;
            this.jsonMessage = 'Unable to start batch.';
        }
    } catch (error) {
        console.error('Error starting batch:', error);
        this.jsonShowSpinner = false;
        this.jsonMessage = 'Failed to start JSON export.';
    }
}

    downloadFile(jsonData, sObjectType, startDate, endDate) {
        const blob = new Blob([jsonData], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `${sObjectType}_${startDate}_to_${endDate}.json`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
}
