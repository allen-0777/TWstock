import os
import json
# from google.cloud import secretmanager
from google.api_core import page_iterator
import logging

import core.CONF as CONF

class CloudServiceInit:

    def __init__(self):
        self.local = CONF.LOCAL
        self.project = os.getenv("PROJECT", default="")

        if self.local:
            # DevKey
            self.creds = None
            # print('Local Environment Set')
        else:
            # Cross-account through secret manager
            if self.project == 'busdata':
                SECRET_MANAGER_URI = "projects/481077910010/secrets/access-masterevbus-gcs"
                self.creds = self.get_secret(SECRET_MANAGER_URI)
            elif self.project == 'stationdata':
                SECRET_MANAGER_URI = "projects/481077910010/secrets/access-masterevbusphihongdatacloud-gcs"
                self.creds = self.get_secret(SECRET_MANAGER_URI)
            else:
                logging.error('No Cross-Account Credentials')

    # Secret Manager
    def get_secret(self, uri):
        client = secretmanager.SecretManagerServiceClient()
        request = {"name": f"{uri}/versions/latest"}
        response = client.access_secret_version(request)
        secret_string = response.payload.data.decode("UTF-8")
        secret_string = json.loads(secret_string)
        return secret_string


# GSC
# class CloudStorage(CloudServiceInit):

#     def __init__(self, local, service_account):
#         self.local = local
#         super().__init__(self.local)
#         # Get Credentcials
#         if service_account is True and self.creds is not None:
#             self.gsclient = storage.Client.from_service_account_info(
#                 self.creds)
#         else:
#             self.gsclient = storage.Client()

#     def _item_to_value(self, iterator, item):
#         return item

#     def list_directories(self, bucket_name, prefix):
#         if prefix and not prefix.endswith('/'):
#             prefix += '/'

#         extra_params = {
#             "projection": "noAcl",
#             "prefix": prefix,
#             "delimiter": '/'
#         }

#         path = "/b/" + bucket_name + "/o"

#         iterator = page_iterator.HTTPIterator(
#             client=self.gsclient,
#             api_request=self.gsclient._connection.api_request,
#             path=path,
#             items_key='prefixes',
#             item_to_value=self._item_to_value,
#             extra_params=extra_params,
#         )

#         return [x for x in iterator]

#     def list_all_blob(self, bucket_name, prefix):
#         # Get GCS bucket
#         bucket = self.gsclient.get_bucket(bucket_name)

#         # Get blobs in bucket (including all subdirectories)
#         blobs_all = list(bucket.list_blobs(prefix=prefix))

#         return blobs_all

#     def download_object(self, bucket_name, object_name, local_file):
#         # Get GCS bucket
#         bucket = self.gsclient.get_bucket(bucket_name)

#         # Download to local file
#         dest = bucket.blob(object_name)

#         try:
#             dest.download_to_filename(local_file)
#         except Exception as e:
#             os.remove(local_file)
#             logging.error(f'Failed Download: {e}')

#     def upload_object(self, bucket_name, local_file, object_name):
#         # Get GCS bucket
#         bucket = self.gsclient.get_bucket(bucket_name)

#         # Write to blob/filename
#         blob = bucket.blob(object_name)
#         blob.upload_from_filename(local_file)


# ###
