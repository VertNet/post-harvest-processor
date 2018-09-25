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

__author__ = "John Wieczorek"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2018 vertnet.org"
__this_file__ = "clean_GCS_folder.py"
__revision_date__ = "2018-09-25T20:41-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

# googleapis has an argparser, which will be invoked when the CloudStorage instance is
# initiated. We must use that argparser and add our argument processing to it.
from googleapis.GoogleAPI import parser
from googleapis import CloudStorage as CS

def get_file_count_from_GCS_folder(cs):
    '''
       Get a count of the files in a bucket on GCS.
    '''
    if cs is None:
        print 'No Google Cloud Storage dispatcher given in %s.' % __version__
    bucket = cs.bucket()
    if bucket is None:
        print 'No Google Cloud Storage bucket given in %s.' % __version__
    
    resp = cs.list_bucket()
#    resp = cs.list_bucket(maxResults=10)
    itemcount = 0
    while resp is not None:
#    if resp is not None:
        if 'items' in resp:
            itemcount += len(resp['items'])
        # Check if there is a page token signifying more items in bucket
        if 'nextPageToken' in resp:
            # Get next page of items
            resp = cs.list_bucket(pageToken=resp['nextPageToken'])
        else:
        	# No more items
            resp = None
    return itemcount

def get_file_list_from_GCS_folder(cs):
    '''
       Get a list of the files in a bucket on GCS.
    '''
    if cs is None:
        print 'No Google Cloud Storage dispatcher given in %s.' % __version__
    bucket = cs.bucket()
    if bucket is None:
        print 'No Google Cloud Storage bucket given in %s.' % __version__
    
    # Make a list of files in the harvest folder
    file_list = []
    resp = cs.list_bucket()
#    resp = cs.list_bucket(maxResults=10)
    while resp is not None:
#    if resp is not None:
        if 'items' in resp:
            for item in resp['items']:
                entry = { 'name':item['name'], 'date':item['timeCreated'] }
                file_list.append( entry )
        # Check if there is a page token signifying more items in bucket
        if 'nextPageToken' in resp:
            # Get next page of items
            resp = cs.list_bucket(pageToken=resp['nextPageToken'])
        else:
        	# No more items
            resp = None
    return file_list

def _getoptions():
    ''' Parse command line options and return them.'''
    # Use imported parser rather than create a new one
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
        print s
        return

    # Create a CloudStorage Manager to be able to access Google Cloud Storage based on
    # the credentials stored in dl_cred.
    cs = CS.CloudStorage( {'bucket_name':options.bucket} )

    # Create a list of folders on Google Cloud Storage to process
    downloadfiles = get_file_list_from_GCS_folder(cs)

    if downloadfiles is None:
        print 'No files found in bucket in %s.' % cs.bucket()
        return

    # Make list of files to remove. There is a string match with date, so files with
    # date less than that given will be removed unless an extact datetime is given
    # (e.g., 2018-09-04T16:57:27.797Z)
    remove_file_list = []
    for file in downloadfiles:
        if file['date'] <= options.date:
            remove_file_list.append(file)
            print 'toss date: %s file: %s' % (file['date'], file['name'])

    # Remove files from remove list
    print '%s files to remove from %s.' % (len(remove_file_list), cs.bucket())
    j = 0
    if options.list_only is None or options.list_only <> 'False':
        for file in remove_file_list:
            cs.delete_object(file['name'])
            j += 1
    print '%s files removed.' % j

    remainingfilecount = get_file_count_from_GCS_folder(cs)
    print '%s files remain in %s.' % (remainingfilecount, cs.bucket())

if __name__ == "__main__":
    main()