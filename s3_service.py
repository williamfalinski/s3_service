import tempfile
import os
import threading
import traceback
import boto3.session
import shutil


class S3Service:
    def __init__(self, bucket_name, profile_name, local_dir=None) -> None:
        self.bucket_name = bucket_name
        self.profile_name = profile_name
        if local_dir is None:
            self.local_dir = tempfile.gettempdir() + '/s3/'
        else:
            self.local_dir = os.path.abspath(local_dir)
        print(f"Using local destination directory: '{self.local_dir}'")

        self.session = boto3.session.Session(profile_name=self.profile_name)
        self.s3_client = self.session.client("s3")
        try:
            print(f"Searching for Bucket: '{self.bucket_name}'")
            self.s3_client.get_bucket_location(Bucket=self.bucket_name)
        except Exception as E:
            raise E
        print('S3 connected')

    def download_s3_prefixes(self, s3_prefixes, replicate_s3_folder_structure=True):
        paginator = self.s3_client.get_paginator('list_objects_v2')
        keys_to_download=[]
        for prefix in s3_prefixes:
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            for page in pages:
                for obj in page['Contents']:
                    print(obj['Key'])
                    keys_to_download.append(obj['Key'])
        return self.download_s3_paths(keys_to_download, replicate_s3_folder_structure, s3_prefixes)

    def download_file(self, sema, s3_path, local_file_path):
        sema.acquire()
        try: 
            print(f"'{local_file_path}'")
            if os.path.isfile(local_file_path):
                sema.release()
                return
            
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            self.s3_client.download_file(Bucket=self.bucket_name, Key=s3_path, Filename=local_file_path)
        except Exception as E:
            print(f'\tDownload error: {s3_path}')
            print(''.join(traceback.TracebackException.from_exception(E).format()) )
            pass
        sema.release()
        return

    def download_s3_paths(self, s3_paths, s3_prefixes=[]):
        threads = []
        maxthreads = 20
        sema = threading.Semaphore(value=maxthreads)
        final_files = []
        print('Downloading files:')
        for s3_path in s3_paths:
            local_file_path = os.path.join(self.local_dir, s3_path)
            local_file_path = local_file_path.replace(' /', '/').replace('/ ', '/')

            final_files.append(local_file_path)
            thread = threading.Thread(
                target=self.download_file, 
                args=[sema, s3_path, local_file_path])
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        print(f"All downloads finished available in: '{self.local_dir}'")
        return final_files

    def delete_s3_local(self):
        if os.path.isdir(self.local_dir):
            shutil.rmtree(self.local_dir)
        print('Deleting local destination directory')
        return

    
    def list_delete_markers(self, prefix=None):
        """
        Lists delete markers in an S3 bucket filtered by prefix and exclude_date.

        :param prefix: The prefix to filter delete markers.
        :return: A list of delete markers (Key and VersionId).
        """
        delete_markers = []
        paginator = self.s3_client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            if 'DeleteMarkers' in page:
                for marker in page['DeleteMarkers']:
                    delete_markers.append({
                        'Key': marker['Key'],
                        'VersionId': marker['VersionId']
                    })
        return delete_markers
    
    def retrieve_deleted_files(self, delete_markers):
        for marker in delete_markers:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=marker['Key'], VersionId=marker['VersionId'])
            print(f"Removed delete marker for {marker['Key']} (VersionId: {marker['VersionId']})")

    
