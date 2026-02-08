import sys
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


account_url = "https://<ADLS>.blob.core.windows.net"
container = '<az-container-name>'
dir_root = '<dir-in-az_container>'
default_credential = DefaultAzureCredential()
fname_prefix = ''

arg_count = len(sys.argv) - 1
assert 2 <= arg_count <= 3, \
    f'Expected 2-3 arguments: (1) source dir (2) target dir (3 optional) file prefix but got {arg_count}'
dir_src = dir_root + sys.argv[1]
dir_trg = dir_root + sys.argv[2]
if arg_count == 3:
    fname_prefix = sys.argv[3]


def extract_blob_fname(blob_name: str) -> str:
    return blob_name.split('/')[-1]


bservice_client = BlobServiceClient(account_url, credential=default_credential)
container_client = bservice_client.get_container_client(container=container)

print('\nSource: ',  dir_src, f'({fname_prefix}*)')
blobs_src = container_client.list_blobs(name_starts_with=dir_src + '/' + fname_prefix)
for blob in blobs_src:
    print(extract_blob_fname(blob.name))

if (dir_src == dir_trg):
    exit('\nTarget is the same as Source')

blobs = container_client.list_blobs(name_starts_with=dir_src + '/' + fname_prefix)
for blob in blobs:
    fname = extract_blob_fname(blob.name)
    print('...moving ', fname)
    blob_name_trg = dir_trg + '/' + fname
    blob_client_src = bservice_client.get_blob_client(container=container, blob=blob.name)
    blob_client_trg = bservice_client.get_blob_client(container=container, blob=blob_name_trg)
    blob_client_trg.start_copy_from_url(blob_client_src.url)
    blob_client_src.delete_blob()

print('\nTarget: ', dir_trg)
blobs_trg = container_client.list_blobs(name_starts_with=dir_trg + '/')
for blob in blobs_trg:
    print(extract_blob_fname(blob.name))
