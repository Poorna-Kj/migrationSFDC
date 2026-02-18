import os
import requests
import base64
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from simple_salesforce import Salesforce

# ---------------------------------------------------
# LOAD ENV
# ---------------------------------------------------
load_dotenv()

SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN")

MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DMS_ENDPOINT = os.getenv("DMS_ENDPOINT")
DMS_AUTH = os.getenv("DMS_AUTH_HEADER").replace('"', '')

SIX_MB = 6 * 1024 * 1024  # 6MB in bytes

print("MONGO_URI USED:", MONGO_URI)

# ---------------------------------------------------
# CONNECT MONGODB
# ---------------------------------------------------
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client["salesforce_dms"]
file_tracking = mongo_db["file_tracking"]

print("MongoDB Connected → salesforce_dms.file_tracking ✅")

# ---------------------------------------------------
# CONNECT SALESFORCE
# ---------------------------------------------------
sf = Salesforce(
    username=SF_USERNAME,
    password=SF_PASSWORD,
    security_token=SF_SECURITY_TOKEN,
    domain=SF_DOMAIN
)

print("Salesforce Connected ✅")


# ---------------------------------------------------
# CONFIGURED SOBJECT LIST
# ---------------------------------------------------
SOBJECT_OPTIONS = [
    "OAgreement__c",
    "OChallan__c",
    "OReceipt__c",
    "OApprovalRequest__c",
    "ONotice__c"
]


# ---------------------------------------------------
# FETCH DISTINCT VERTICAL VALUES
# ---------------------------------------------------
def get_vertical_options(object_name):

    query = f"""
        SELECT Vertical__c
        FROM {object_name}
        WHERE Vertical__c != null
        GROUP BY Vertical__c
    """

    try:
        results = sf.query_all(query)
        verticals = list(set(
            record['Vertical__c']
            for record in results['records']
            if record.get('Vertical__c')
        ))
        return verticals
    except:
        return []


# ---------------------------------------------------
# FETCH FILES
# ---------------------------------------------------
def fetch_files(vertical, object_name, start_date, end_date):

    print("\n🔎 Fetching ContentVersion records (Date + Size Filter)...")

    version_query = f"""
        SELECT Id, Title, FileType, ContentDocumentId,
               CreatedDate, VersionData, ContentSize
        FROM ContentVersion
        WHERE IsLatest = true
        AND CreatedDate >= {start_date}T00:00:00Z
        AND CreatedDate <= {end_date}T23:59:59Z
        AND ContentSize > {SIX_MB}
    """

    version_results = sf.query_all(version_query)
    all_versions = version_results['records']

    print(f"Total Files Found (>6MB + Date Filter): {len(all_versions)}")

    if not all_versions:
        return []

    filtered_files = []

    print("\n🔎 Filtering by Object + Vertical...")

    for file in all_versions:

        doc_id = file["ContentDocumentId"]

        link_query = f"""
            SELECT LinkedEntityId
            FROM ContentDocumentLink
            WHERE ContentDocumentId = '{doc_id}'
        """

        links = sf.query_all(link_query)

        for link in links['records']:

            parent_id = link["LinkedEntityId"]

            parent_query = f"""
                SELECT Id
                FROM {object_name}
                WHERE Id = '{parent_id}'
                AND Vertical__c = '{vertical}'
                LIMIT 1
            """

            parent_check = sf.query(parent_query)

            if parent_check['records']:
                filtered_files.append(file)
                break

    print(f"Final Files After Vertical Filter: {len(filtered_files)}")

    return filtered_files


# ---------------------------------------------------
# PUSH TO DMS
# ---------------------------------------------------
def push_to_dms(file_record):

    file_id = file_record["Id"]
    title = file_record["Title"]
    file_type = file_record["FileType"]
    version_data_url = file_record["VersionData"]

    if file_tracking.find_one({"salesforce_file_id": file_id}):
        print(f"⏭ Skipping (Already Migrated): {title}")
        return

    try:
        file_binary = sf._call_salesforce("GET", version_data_url)
        encoded_file = base64.b64encode(file_binary).decode()

        payload = {
            "fileName": title,
            "fileType": file_type,
            "fileData": encoded_file
        }

        headers = {
            "Authorization": DMS_AUTH,
            "Content-Type": "application/json"
        }

        response = requests.post(
            DMS_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=120
        )

        if response.status_code == 200:
            print(f"✅ Uploaded: {title}")

            file_tracking.insert_one({
                "salesforce_file_id": file_id,
                "file_name": title,
                "uploaded_at": datetime.utcnow(),
                "status": "SUCCESS"
            })
        else:
            print(f"❌ Failed: {title}")

    except Exception as e:
        print(f"❌ Exception uploading {title}: {str(e)}")


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------
if __name__ == "__main__":

    print("\nSelect Object:")
    for i, obj in enumerate(SOBJECT_OPTIONS):
        print(f"{i+1}. {obj}")

    obj_choice = int(input("Enter choice number: "))
    object_input = SOBJECT_OPTIONS[obj_choice - 1]

    verticals = get_vertical_options(object_input)

    if not verticals:
        print("No Vertical values found.")
        exit()

    print("\nSelect Vertical:")
    for i, v in enumerate(verticals):
        print(f"{i+1}. {v}")

    vert_choice = int(input("Enter choice number: "))
    vertical_input = verticals[vert_choice - 1]

    start_date_input = input("Enter Start Date (YYYY-MM-DD): ")
    end_date_input = input("Enter End Date (YYYY-MM-DD): ")

    files = fetch_files(
        vertical_input,
        object_input,
        start_date_input,
        end_date_input
    )

    if not files:
        print("\n⚠ No files to migrate.")
        exit()

    print("\n🚀 Starting Migration...\n")

    for file in files:
        push_to_dms(file)

    print("\n🎯 Migration Completed Successfully")
