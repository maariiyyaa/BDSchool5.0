from json import load
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError

with open('conf.json') as config:
    conf_data = load(config)
    service = DataLakeServiceClient(account_url = conf_data['account_url'],
                                    storage_account_name = conf_data['storage_account_name'],
                                    credential = conf_data['credential'])
    try:
        container = service.create_file_system(file_system=conf_data['file_system'])
    except ResourceExistsError:
        print("Container already exists. Connecting to it")
        container = service.get_file_system_client(file_system=conf_data['file_system'])


def create_dir(dir_name: str) -> str:
    """ Function creates a chain of folders from names in list

    :param dir_name: Full path for newly created folders
    :return: Path to new directory
    """
    try:
        container.create_directory(dir_name)
    except ResourceExistsError:
        print("Directory already exists.")
    return dir_name


def upload_file(dir_name: str, file_name: str) -> None:
    """ Function uploads file

    :param dir_name: Directory where the file will be loaded
    :param file_name: Name of file to be uploaded
    """
    try:
        directory_client = container.get_directory_client(dir_name)
        file_client = directory_client.create_file(file_name)
        with open(file_name, 'rb') as file_contents:
            file_client.append_data(data=file_contents.read(), offset=0, length=len(file_contents.read()))
            file_client.flush_data(len(file_contents.read()))
    except ResourceExistsError:
        print("This file already exists.")


def upload_and_put_file(dir_name: str, directory_for_upload_index: int, file_name: str) -> None:
    """ Function calls create_dir to create directory structure. Than file uploads to specified folder

    :param dir_name: Full path for newly created folders
    :param directory_for_upload_index: Index of folder ( in dir_names splitted by '\' list ) where file will be uploaded
    :param file_name: Name of file to be uploaded
    """
    full_path = create_dir(dir_name)
    upload_directory = ('\\'.join(full_path.split('\\')[:directory_for_upload_index]))
    upload_file(upload_directory, file_name)
    print("File is uploaded sucesfully")

def main():
    upload_and_put_file('Dir11\\Dir22\\Dir33\\Dir44\\Dir55', 4, "BondarMariia_test.csv")


if __name__ == '__main__':
    main()
