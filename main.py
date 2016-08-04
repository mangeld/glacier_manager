import boto3
from os.path import getsize
from botocore.utils import calculate_tree_hash

class GlacierUploader:

    CHUNK_SIZE = (1024 ** 2) * 32
    RANGE_FORMAT = "Content-Range: bytes {}-{}/*"

    def __init__(self, filename, vault):
        self.file_handle = open(filename, 'rb')
        self.vault = vault
        self.glacier = boto3.client("glacier")
        self.multipart_upload_id = None
        self.file_size = getsize(filename)

    def start_multipart_upload(self):
        response = self.glacier.initiate_multipart_upload(
            vaultName=self.vault,
            archiveDescription=self.file_handle.name,
            partSize=str(self.CHUNK_SIZE),
        )
        self.multipart_upload_id = response.get('uploadId')

    @property
    def multipart_upload_ids(self):
        response = self.glacier.list_multipart_uploads(vaultName=self.vault)
        return [i.get('MultipartUploadId') for i in response.get('UploadsList')]

    def cancel_all_multipart_uploads(self):
        for upload_id in self.multipart_upload_ids:
            self.glacier.abort_multipart_upload(
                vaultName=self.vault,
                uploadId=upload_id
            )

    def upload_chunk(self):
        last_bytes = self.file_handle.tell()
        next_bytes = (last_bytes + self.CHUNK_SIZE) - 1
        if next_bytes > self.file_size:
            import ipdb; ipdb.set_trace()
            next_bytes = self.file_size - 1
        content_range = "bytes {}-{}/{}".format(last_bytes, next_bytes, self.file_size)
        self.glacier.upload_multipart_part(
            vaultName=self.vault,
            uploadId=self.multipart_upload_id,
            range=content_range,
            body=self.file_handle.read(self.CHUNK_SIZE),
        )
        return self.file_handle.tell()

    def upload(self):
        self.start_multipart_upload()
        while True:
            last_writed = self.upload_chunk()
            if last_writed == self.file_size:
                self.finalize_upload()
                break

    def finalize_upload(self):
        self.file_handle.seek(0)
        filehash = calculate_tree_hash(self.file_handle)
        self.glacier.complete_multipart_upload(
            vaultName=self.vault,
            uploadId=self.multipart_upload_id,
            archiveSize=str(self.file_size),
            checksum=filehash,
        )
