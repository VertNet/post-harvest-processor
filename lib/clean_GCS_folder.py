#!/usr/bin/env python
# -*- coding: utf-8 -*-
# The line above is to signify that the script contains utf-8 encoded characters.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Adapted from check_harvest_folder_GCS.py
# Adapted from get_harvest_folders.py
# Updated to Google Cloud Storage Client

__author__ = "John Wieczorek"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2020 vertnet.org"
__this_file__ = "clean_GCS_folder2.py"
__revision_date__ = "2020-11-28T18:24-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

import argparse
from datetime import datetime
from google.cloud import storage

def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser()
    help = 'Earliest file date to keep (e.g., 2018-06-23; required)'
    parser.add_argument("-d","--date", help=help, required=True)
    help = 'Google Cloud Storage bucket name (e.g., vn-downloads2; required)'
    parser.add_argument("-b","--bucket", help=help, required=True)
    help = 'List only, do not delete (e.g., True; optional)'
    parser.add_argument("-l","--list_only", help=help, required=False)
    return parser.parse_args()

def main():
    ''' 
    Remove from a Google Cloud Storage bucket files that are older than date provided.

    Invoke with date, bucket, list_only parameters as:
       python clean_GCS_folder.py -d 2018-06-23 -b vn-downloads2
       or
       python clean_GCS_folder.py -d 2018-06-23 -b vn-downloads2 -l True
    '''
    options = _getoptions()

    if options.date is None or len(options.date)==0 or \
       options.bucket is None or len(options.bucket)==0:
        s =  'syntax:\n'
        s += 'python %s' % __this_file__
        s += ' -d 2018-06-23'
        s += ' -b vn-downloads2'
        s += ' -l True'
        print(s)
        return

    # Input earliest date to keep must be a a W3C datetime
    try:
        delete_before_date=datetime.fromisoformat(options.date)
    except ValueError as e:
        print(e)
        return

    # Create a CloudStorage Manager to be able to access Google Cloud Storage based on
    # the credentials stored in dl_cred.
    # cs = CS.CloudStorage( {'bucket_name':options.bucket} )
    cs = storage.Client()
    try:
      bucket = cs.get_bucket(options.bucket)
    except google.cloud.exceptions.NotFound:
      print("Sorry, the bucket %s does not exist!" % options.bucket)
    all_blobs_list = cs.list_blobs(bucket)
    blob_count=len(list(all_blobs_list))
    print("Before: Bucket %s has %s files." % (bucket, blob_count))
    remove_list=[]
    all_blobs_list = cs.list_blobs(bucket)
    for blob in all_blobs_list:
        if blob.time_created.replace(tzinfo=None)<delete_before_date:
          remove_list.append(blob)
    print("%s files to remove from before %s." % (len(remove_list),options.date))
    bucket.delete_blobs(remove_list)
    all_blobs_list = cs.list_blobs(bucket)
    blob_count=len(list(all_blobs_list))
    print("After: Bucket %s has %s files." % (bucket, blob_count))

if __name__ == "__main__":
    main()