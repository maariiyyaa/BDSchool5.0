
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
service = DataLakeServiceClient(account_url="https://bdschool002.dfs.core.windows.net/",
                                storage_account_name = 'bdschool002',
                                credential = "dgh/Q9jVVJFbhcTSvZYpJ3hs/1prlXHGPIrs00sD7Ry3xVOgyEIuI2BRrMJEjHN0a9n0cT/J3ooCvOV7QtEwrw==")


def CreateIncludedDir(DirName): #creating a chain of folders
    try:
         new_container.create_directory(DirName)
    except ResourceExistsError:
        print("Directory already exists.")

    return DirName
    
    
def UploadFile(DirName, Filename): #uploading a file to the directory
    try:
        directory_client = new_container.get_directory_client(DirName)
        file_client = directory_client.create_file(Filename)
        file_contents = open(Filename,'rb').read()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
    except ResourceExistsError:
        print("This file already exists.")


def CreateChain_UploadFile(Chain, NumDirFromCor, Filename): #creating a chain of folders and uploading a file to a specific position
    chain = CreateIncludedDir(Chain)
    pleceToUpload = ('\\'.join(chain.split('\\')[:NumDirFromCor]))
    UploadFile(pleceToUpload, Filename)


def main():
    try:
        new_container = service.create_file_system(file_system="testcontainer")
    except ResourceExistsError:
        print("Container already exists.")
        new_container = service.get_file_system_client(file_system="testcontainer")

    CreateChain_UploadFile('Dir11\\Dir22\\Dir33\\Dir44\\Dir55', 3, "BondarMariia_test.csv")

if __name__ == '__main__':
    main()

