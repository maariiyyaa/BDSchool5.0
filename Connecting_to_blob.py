from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

# Connect to blob via sharing key
Blob_service = BlobServiceClient(account_url="https://*.blob.core.windows.net/",
                            credential="credential")

try:
    new_container = Blob_service.create_container("container001")
    properties = new_container.get_container_properties()
except ResourceExistsError:
    print("Container already exists.")

try:
    all_containers = Blob_service.list_containers(include_metadata=True)
    for container in all_containers:
        print(container['name'], container['metadata'])

except ResourceNotFoundError:
    print("Container is not founded.")

try:
    with open("BondarMariia_test.csv", "rb") as data:
        new_container.upload_blob(name ='yellow_tripdata_2020-01.csv', data=data)
except ResourceExistsError:
    print("blob already exists.")

try:
    Blob_service.delete_container("container001")
except ResourceNotFoundError:
    print("Container already deleted.")


# Connect to blob via SAS
from azure.storage.blob import BlobServiceClient

connection_string = "BlobEndpoint=https://*.blob.core.windows.net/;SharedAccessSignature=*"
service = BlobServiceClient.from_connection_string(conn_str=connection_string)

try:
    new_container = service.create_container("container002")
    properties = new_container.get_container_properties()
except ResourceExistsError:
    print("Container already exists.")

try:
    with open("IndianFoodDatasetCSV.csv", "rb") as data:
        new_container.upload_blob(name ='IndianFoodDatasetCSV.csv', data=data)
except ResourceExistsError:
    print("blob already exists.")


