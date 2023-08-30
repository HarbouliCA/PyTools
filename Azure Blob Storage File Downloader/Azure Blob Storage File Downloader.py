from azure.storage.blob import BlobServiceClient, BlobClient
from requests.exceptions import ConnectionError, Timeout
import os

# Configuration
connection_string = "Your_Connection_String"
resource_group = "resource_group"
storage_account_name = "storage_account_name"
filesystem_name = "filesystem_name"
destination_folder = r"destination_folder"

# Initialize BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(filesystem_name)

# Make destination directory if not exists
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# List blobs in the container
blobs = list(container_client.list_blobs())
total_blobs = len(blobs)
downloaded_count = 0

for blob in blobs:
    blob_client = blob_service_client.get_blob_client(container=filesystem_name, blob=blob.name)
    download_file_path = os.path.join(destination_folder, blob.name.replace('/', os.path.sep))

    # Check if the file already exists
    if os.path.exists(download_file_path):
        print(f"File {blob.name} already exists. Skipping download.")
        continue

    dir_name = os.path.dirname(download_file_path)
    
    # Check if the directory is a file. If it is, rename the file and proceed with directory creation.
    if os.path.isfile(dir_name):
        os.rename(dir_name, dir_name + '_backup')
    
    # Create the directory if it doesn't exist, right before writing the file
    os.makedirs(dir_name, exist_ok=True)

    try:
        with open(download_file_path, "wb") as download_file:
            downloader = blob_client.download_blob()
            download_file.write(downloader.readall())
            downloaded_count += 1
            print(f"Downloaded: {blob.name}")

    except (ConnectionError, Timeout, Exception) as e:
        print(f"An error occurred while downloading {blob.name}. Error: {e}")
    
    # Print the percentage of completion
    percentage_completion = (downloaded_count / total_blobs) * 100
    print(f"Progress: {percentage_completion}%")

print("Sync completed!")
print(f"Total files: {total_blobs}")
print(f"Downloaded files: {downloaded_count}")
print(f"Pending files: {total_blobs - downloaded_count}")