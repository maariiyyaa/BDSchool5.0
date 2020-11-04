
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

service = DataLakeServiceClient(account_url="https://bdschool002.dfs.core.windows.net/",
                                storage_account_name = 'bdschool002',
                                credential = "dgh/Q9jVVJFbhcTSvZYpJ3hs/1prlXHGPIrs00sD7Ry3xVOgyEIuI2BRrMJEjHN0a9n0cT/J3ooCvOV7QtEwrw==")


def CreateIncludeDir(DirName, Depth): #creating a chain of folders by inputing names
    
    try:
        directory_client = new_container.create_directory(DirName)
    except ResourceExistsError:
        print("Directory already exists.")
        
    Depth = Depth - 1
    if Depth > 0:
        newdirname = input()
        DirName = DirName+'\\'+ newdirname
        DirName = CreateIncludeDir(DirName, Depth)
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


def CreateChain_UploadFile(root, fooldepth, NumDirToUpl, FileName): #creating a chain of folders and uploading a file to a specific position
    
    chain = CreateIncludeDir(root,fooldepth)
    pleceToUpload = ('\\'.join(chain.split('\\')[:NumDirToUpl]))
    UploadFile(pleceToUpload, FileName)




def main():

    try:
        new_container = service.create_file_system(file_system="testcontainer")
    except ResourceExistsError:
        print("Container already exists.")
        new_container = service.get_file_system_client(file_system="testcontainer")


    CreateChain_UploadFile('Dir1', 5, 4, "BondarMariia_test.csv")

if __name__ == '__main__':
    main()



