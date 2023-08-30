from azure.storage.blob import BlobServiceClient
import os

# Provide the folder path on your desktop where the images are stored
folder_path = r"folder_path"

# Connection string to your Azure Storage Account
connection_string = "connection_string"

# Name of the container where you want to store the images
container_name = "container_name"

def upload_images_to_azure(folder_path, connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Create container if it doesn't exist
    try:
        container_client.create_container()
        print(f"Container '{container_name}' created.")
    except:
        print(f"Container '{container_name}' already exists.")

    upload_count = 0

    print(f"Listing files in folder: {folder_path}")
    # Iterate through each file in the folder
    for filename in os.listdir(folder_path):
        print(f"Checking file: {filename}")
        # Check if file is an image by looking at the file extension
        if filename.endswith(('.PNG', '.jpg', '.jpeg', '.gif', '.bmp')):
            file_path = os.path.join(folder_path, filename)
            print(f"Uploading image: {file_path}")

            blob_client = container_client.get_blob_client(filename)

            # Upload the image to Azure Storage
            with open(file_path, "rb") as image_file:
                blob_client.upload_blob(image_file)
                upload_count += 1
                print(f'{filename} has been uploaded.')

    print(f'Total number of images uploaded: {upload_count}')



upload_images_to_azure(folder_path, connection_string, container_name)