import posixpath

from google.cloud import storage as gcs
from threading import local
from django.core.files.storage import Storage
from tempfile import SpooledTemporaryFile
from django.core.files.base import File
from io import TextIOBase, BytesIO
from PIL import Image, ImageOps

from django.conf import settings
from django.utils.deconstruct import deconstructible
from django.utils.encoding import force_bytes, force_str
from django.utils.timezone import make_naive, utc
from django.contrib.staticfiles.storage import ManifestFilesMixin

from ..settings import api_settings


acl_mapping = {
    "private": lambda acl: acl.all().revoke_read()  ,
    "publicRead": lambda acl: acl.all().grant_read(),
    "publicReadWrite": lambda acl: acl.all().grant_read().grant_write(),
    "authenticatedRead": lambda acl: acl.all().grant_read().grant_authenticated_read(),
    "bucketOwnerRead": lambda acl, bucket_owner: acl.all().grant_read(bucket_owner),
    "bucketOwnerFullControl": lambda acl, bucket_owner: acl.all().grant_full_control(bucket_owner),
}

def _temporary_file():
    return SpooledTemporaryFile(max_size=1024 * 1024 * 50)  # 10 MB.

def _to_sys_path(name):
    return name.replace("/", os.sep)


def _to_posix_path(name):
    return name.replace(os.sep, "/")


class GCSFile(File):

    """
    A file returned from Google Cloud Storage.
    """

    def __init__(self, file, name, storage):
        super(GCSFile, self).__init__(file, name)
        self._storage = storage

    def open(self, mode="rb"):
        if self.closed:
            self.file = self._storage.open(self.name, mode).file
        return super(GCSFile, self).open(mode)

class _Local(local):

    """
    Thread-local connection manager for Google Cloud Storage.
    """

    def __init__(self, project_id, bucket_name):
        self.client = gcs.Client(project = project_id)
        self.bucket = self.client.get_bucket(bucket_name)


@deconstructible
class GCSStorage(Storage):

    """
    An implementation of Django file storage over Google Cloud Storage.

    """
    KEY_PREFIX = api_settings.ATTACHED_FILE_GCS_KEY_PREFIX
    BUCKET_NAME = api_settings.ATTACHED_FILE_GCS_BUCKET_NAME
    MEDIA_URL = api_settings.ATTACHED_FILE_MEDIA_URL
    PROJECT_ID = api_settings.GOOGLE_PROJECT_ID
    RESIZE = False
    ACL = "private"

    @property
    def gcs_client(self):
        return self._connection.client


    def __init__(self, **kwargs):
        print("Initializing the GCS storage for csv")
        self._kwargs = kwargs
        self._base_url = None
        self._project_id = self.PROJECT_ID
        self._connection = _Local(self._project_id, self.BUCKET_NAME)
        self._acl = self.ACL
        super(GCSStorage, self).__init__()
        print("initialization complete, the object created  is  ", vars(self))
    
    def _object_params(self, name):
        params = {
            "Bucket": self.BUCKET_NAME,
            "Key": self._get_key_name(name),
        }
        return params

    def _object_put_params(self, name):
        # Set basic params.
        params = {
            "ACL": "private"
        }
        params.update(self._object_params(name))
        return params
    
    def _set_ACL_params(self,bucket,blob,acl= "private"):
        
        if acl in acl_mapping:
            # Use the mapped ACL value to set the ACL for the GCS blob
            acl_mapping[acl](blob.acl)
        else:
            
            acl_mapping[acl](blob.acl)  
        # Save the ACL changes
        print("saving ACL changes")
        blob.acl.save()
        print("ACL changes saved successfully")

    
    def _is_ACL_enabled(self,bucket):
        acls = list(bucket.acl)
        if acls:
            return True
        else:
            return False
                
            
    def _get_key_name(self, name):
        if name.startswith("/"):
            name = name[1:]
        return posixpath.normpath(posixpath.join(self.KEY_PREFIX, _to_posix_path(name)))

    def _open(self, name, mode="rb"):
            if mode != "rb":
                raise ValueError("GCS files can only be opened in read-only mode")
            blob = self.gcs_client.bucket(self.BUCKET_NAME).get_blob(name)
            if not blob:
                raise FileNotFoundError(f"The file {name} does not exist in the bucket.")
            content = _temporary_file()
            blob.download_to_file(content)
            content.seek(0)

            # Un-gzip if required.
            # if obj.get("ContentEncoding") == "gzip":
            #    content = gzip.GzipFile(name, "rb", fileobj=content)
            # All done!
            return GCSFile(content, name, self)
        
    def _save(self, name, content):
        print(f'Calling the save method with name as {name}')
        temp_files = []
        bucket = self.gcs_client.bucket(self.BUCKET_NAME)
        # The Django file storage API always rewinds the file before saving,
        # therefor so should we.
        content.seek(0)
        # Convert content to bytes.
        if isinstance(content.file, TextIOBase):
            temp_file = _temporary_file()
            temp_files.append(temp_file)
            for chunk in content.chunks():
                temp_file.write(force_bytes(chunk))
            temp_file.seek(0)
            content = temp_file
        if self.RESIZE:
            with Image.open(content) as img:
                image_size = (512, 512)
                if img.width > image_size[0] or img.height > image_size[0]:
                    thumb = ImageOps.fit(img, image_size, Image.ANTIALIAS)
                    img_bytes = BytesIO()
                    thumb.save(img_bytes, format=img.format)
                    img_byte_array = img_bytes.getvalue()
                    blob = bucket.blob(self.KEY_PREFIX+name)
                    print(f'Setting the ACL as {self._acl}')
                    print("uploading blob to bucket")
                    blob.upload_from_string(img_byte_array, content_type=Image.MIME[img.format])
                    if self._is_ACL_enabled(bucket):
                        print("ACL enabled")
                        self._set_ACL_params(self.BUCKET_NAME,blob,self._acl)
                    print("Upload completed successfully")
        else:
            blob = self.gcs_client.bucket(self.BUCKET_NAME).blob(self.KEY_PREFIX+name)
            print(f'Setting the ACL as {self._acl}')
            blob.upload_from_string(content.read())
            if self._is_ACL_enabled(self.gcs_client.bucket(self.BUCKET_NAME)):
                self._set_ACL_params(self.BUCKET_NAME,blob,self._acl)
            self._set_ACL_params(self.BUCKET_NAME,blob,self._acl)
            print("Upload completed successfully")

        # Close all temp files.
        for temp_file in temp_files:
            temp_file.close()
        # All done!
        return name        
    
    def meta(self, name):
        """Returns a dictionary of metadata associated with the key."""
        return self.gcs_client.get_bucket(self.BUCKET_NAME).blob(name).metadata
    
    def delete(self, name):
       self.gcs_client.get_bucket(self.BUCKET_NAME).blob(name).delete()
    def exists(self, name):
        blob = self.gcs_client.bucket(self.BUCKET_NAME).get_blob(name)
        return blob is not None

    def listdir(self, path):
        #In GCS, unlike AWS we only have flat name hierarchy. So there is no need to worry about how 
        # the path is handled.
        # Look through the paths, parsing out directories and paths.
        files = []
        dirs = []
        # Get the reference to the bucket
        bucket = self.gcs_client.bucket(self.BUCKET_NAME)
        blobs = bucket.list_blobs(prefix=path, delimiter="/")

        for blob in blobs:
            if isinstance(blob, gcs.Blob):
                # It's a file
                files.append(posixpath.relpath(blob.name, path))
            elif isinstance(blob, gcs.Prefix):
                # It's a directory (prefix in GCS)
                dirs.append(posixpath.relpath(blob.name, path))

        # All done!
        return dirs, files
    
    def size(self, name):
        return self.meta(name)["ContentLength"]

    def url(self, name):
        return f"{self.base_url}/{self.KEY_PREFIX}/{name}"

    def modified_time(self, name):
        return make_naive(self.meta(name)["LastModified"], utc)

    created_time = accessed_time = modified_time

    def get_modified_time(self, name):
        timestamp = self.meta(name)["LastModified"]
        return timestamp if settings.USE_TZ else make_naive(timestamp)

    get_created_time = get_accessed_time = get_modified_time
    
class StaticGCSStorage(GCSStorage):
    """
    A GCS storage for storing static files.
    """

    def path(self, name):
        pass  # not required now

    KEY_PREFIX = ""
    BUCKET_NAME = api_settings.GCS_BUCKET_NAME
    MEDIA_URL = api_settings.GCS_IMAGE_MEDIA_URL
    RESIZE = False
    ACL = "publicRead"

    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self._base_url = None
        self.KEY_PREFIX = kwargs.get("key")
        self.RESIZE = kwargs.get("resize")
        self.ACL = kwargs.get("acl",None)
        super(StaticGCSStorage, self).__init__()
        

class ManifestStaticGCSStorage(ManifestFilesMixin, GCSStorage):
    def post_process(self, *args, **kwargs):
        try:
            for r in super(ManifestStaticGCSStorage, self).post_process(*args, **kwargs):
                yield r
        finally:
            pass  # do nothing