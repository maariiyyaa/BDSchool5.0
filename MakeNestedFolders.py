from json import load
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core.exceptions import ResourceExistsError
from typing import List

with open('conf.json') as config:
    conf_data = load(config)
    service = DataLakeServiceClient(account_url= conf_data['account_url'],
                                    storage_account_name = conf_data['storage_account_name'],
                                    credential = conf_data['credential'])
    try:
        container = service.create_file_system(file_system=conf_data['file_system'])
    except ResourceExistsError:
        print("Container already exists. Connecting to it")
        container = service.get_file_system_client(file_system=conf_data['file_system'])


def create_dir_recursive(dir_names: List[str] , base_path: str = '' ) -> str:
    """ Function creates a chain of folders from names in list

    :param base_path: String contains path for new folder creation
    :param dir_names: List of names for folders to be created
    :return: Path to deepest directory
    """
    try:
        container.create_directory(base_path + dir_names[0])
    except ResourceExistsError:
        print("Directory already exists.")

    if len(dir_names) == 1:
        try:
            container.create_directory(base_path + dir_names[0])
        except ResourceExistsError:
            print("Directory already exists.")

        return base_path

    base_path = create_dir_recursive(dir_names[1:], base_path + dir_names[0] +'\\')
    return base_path


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


def upload_and_put_file(dir_names: List[str], directory_for_upload_index: int, file_name: str) -> None:
    """ Function calls create_dir_recursive to create directory structure. Than file uploads to specified folder

    :param dir_names: List of names for folders to be created
    :param directory_for_upload_index: Index of folder ( in dir_names list )  where file will be uploaded
    :param file_name: Name of file to be uploaded
    """
    full_path = create_dir_recursive(dir_names)
    upload_directory = ('\\'.join(full_path.split('\\')[:directory_for_upload_index]))
    upload_file(upload_directory, file_name)
    print("File is uploaded sucesfully")


def main():
    upload_and_put_file(['dir' + str(i) for i in range(5)], 4, "BondarMariia_test.csv")


if __name__ == '__main__':
    main()
