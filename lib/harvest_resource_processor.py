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

# Adapted from https://github.com/VertNet/bigquery

__author__ = "John Wieczorek"
__contributors__ = "John Wieczorek, Javier Otegui"
__copyright__ = "Copyright 2018 vertnet.org"
__this_file__ = "harvest_resource_processor.py"
__revision_date__ = "2018-09-25T14:29-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

# googleapis has an argparser, which will be invoked when the CloudStorage instance is
# initiated. We must use that argparser and add our argument processing to it.
from googleapis.GoogleAPI import parser
from googleapis import CloudStorage as CS
from google_creds import cs_cred
from harvest_record_processor import VertNetHarvestFileProcessor
from harvest_utils import get_harvest_folders_from_file
from field_utils import index_fields
from subprocess import call
from datetime import datetime
from carto_utils import carto_query
import os
import time

def process_harvest_folders(cs, harvestfolders, processor):
    for f in harvestfolders:
        # Time how long it takes to process the folder
        start = time.time()
        # Process all of the files in the folder
        process_harvest_folder(cs, processor, f)
        end = time.time()
        print '%s to process %s for %s.' % ((end - start), f['gbifdatasetid'], f['icode'])

def process_harvest_folder(cs, processor, row):
    """Process a single harvest folder."""
    today = datetime.today().isoformat().split('T')[0]

    if row.has_key('harvestfolder') == False or len(row['harvestfolder'].strip())==0:
        print 'No harvest folder for %s %s %s' % (row['icode'], row['gbifdatasetid'])
        return False

    bucket = row['harvestfolder'].split('/', 1)[0]
    resource = row['harvestfolder'].split('/', 1)[1]

    params = {}
    params['bucket'] = bucket
    params['icode'] = row['icode']
    params['gbifdatasetid'] = row['gbifdatasetid']
    params['today'] = today

    # Make a list of files in the harvest folder
    uri_list = []
    for item in cs.list_bucket(prefix=resource)['items']:
        # Failure occurred here once when the file names in Cloud Storage began with '/'.
        uri = '/'.join(["gs:/", bucket, item['name']])
        uri_list.append(uri)

    if len(uri_list) == 0:
        return False

    localinputfolder = '%s/in' % resource
    localoutputfolder = '%s/out' % resource

    # Create working folder with input and output directories if they do not already exist
    if not os.path.exists(localinputfolder):
        os.makedirs(localinputfolder)
    if not os.path.exists(localoutputfolder):
        os.makedirs(localoutputfolder)

    # Download the shard files from the harvest folder
    if download_files(localinputfolder, uri_list) == False:
        print 'Did not complete download mission for %s' % row
        return False

    # Process each downloaded shard file into the output folder
    if process_files(processor, localinputfolder, localoutputfolder) == False:
        print 'Did not complete processing mission for %s' % row
        return False

    # Upload processed shards to Google Cloud Storage
    if upload_files(localoutputfolder, params) == False:
        print 'Did not complete uploading mission for %s' % row
        return False

    return True

def process_files(processor, inputfolder, outputfolder):
    '''
       Process each file in the input folder for indexing and put the results in the 
       output folder.
    '''
    # Get the list of input files
    filelist = os.listdir(inputfolder)
    if '.DS_Store' in filelist:
        filelist.remove('.DS_Store')

    i = 0
    # Process each input file and put the results into the output folder
    for f in filelist:
        inputfile = '%s/%s' % (inputfolder, f)
        outputfile = '%s/%s' % (outputfolder, f)
        processor.parse_harvest_file(inputfile, outputfile, 'noheader')
        i += 1
        print '%s of %s) processed to %s' % (i, len(filelist), outputfolder)
    return True

def download_files(destination, uri_list):
    '''
       Download the files in the uri_list into the folder specified by dest.
    '''
    # Get a list of files in the destination folder, ignoring .DS_Store on Macs
    filelist = os.listdir(destination)
    if '.DS_Store' in filelist:
        filelist.remove('.DS_Store')

    i = 0
    # If the destination folder is empty, download the files
    if len(filelist)==0:
        for uri in uri_list:
            # Download file from uri
            result = call(['gsutil', 'cp', uri, destination])
            if result != 0:
                print 'The system call to gsutil produced error code %s' % result
                return False
            i += 1
            print '%s) downloaded %s to %s' % (i, uri, destination)
    else:
        print '*** Skipping %s, local folder is not empty' % (destination)
        return False

def upload_files(source, params):
    '''
       Upload the files in the given source folder to the location specified by params.
    '''
    # Get the list of files to upload
    filelist = os.listdir(source)
    if '.DS_Store' in filelist:
        filelist.remove('.DS_Store')

    i = 0
    # If there are files in the folder
    if len(filelist)>0:
        bucket = params['bucket']
        icode = params['icode']
        gbifdatasetid = params['gbifdatasetid']
        today = params['today']
    
        for file in filelist:
            # Upload file to Google Cloud Storage
            orig = '%s/%s' % (source, file)
            dest = 'gs://%s/processed/%s/%s/%s-%s' \
                % (bucket, icode, gbifdatasetid, today, file)
            result = call(['gsutil', '-m', 'cp', orig, dest])
            if result != 0:
                print 'The system call to gsutil produced error code %s' % result
                return False
            i += 1
            print '%s of %s) uploaded %s to %s' % (i, len(filelist), orig, dest)
    else:
        print '*** No files to process in %s' % (folder)
        return False
    return True

def _getoptions():
    ''' Parse command line options and return them.'''
    # Use imported parser rather than create a new one
#    parser = argparse.ArgumentParser()

    help = 'Carto REST API URL; required)'
    parser.add_argument("-u","--url", help=help, required=True)

    help = 'API key for Carto; required)'
    parser.add_argument("-c","--carto_api_key", help=help, required=True)

    help = 'Target GCS bucket; required)'
    parser.add_argument("-b","--bucket", help=help, required=True)
    return parser.parse_args()

def main():
    ''' 
    Get the folders to process from the results of a Carto query 
    (modified to filter on a particular harvestfolder, for example):
      SELECT icode, gbifdatasetid, harvestfolder
      FROM resource_staging b
      WHERE 
      ipt=True AND 
      networks like '%VertNet%' AND
      harvestfolder LIKE 'vertnet-harvesting/data/2018-09-21/%'
      order by icode, github_reponame asc

    Invoke with bucket parameter as:
       python check_harvest_folder_GCS.py /
         -u https://vertnet.carto.com/api/v2/sql /
         -c [carto_api_key] /
         -b vertnet-harvesting/data/2018-09-21/%
    '''
    options = _getoptions()

    if options.carto_api_key is None or len(options.carto_api_key)==0 or \
       options.url is None or len(options.url)==0 or \
       options.bucket is None or len(options.bucket)==0:
        s =  'syntax:\n'
        s += 'python %s ' % __this_file__
        s += '-u https://vertnet.carto.com/api/v2/sql '
        s += '-c [carto_api_key '
        s += '-b vertnet-harvesting/data/2018-09-21/%'
        print s
        return

    q =  "SELECT icode, gbifdatasetid, harvestfolder "
    q += "FROM resource_staging "
    q += "WHERE "
    q += "ipt=True AND networks like '%VertNet%' AND "
    q += "harvestfolder LIKE '%s' " % options.bucket
    q += "order by icode, github_reponame asc"

    # Try getting the harvest folders directly from Carto resource_staging table matching 
    # harvestfolder.
    harvestfolders = carto_query(options.url, options.carto_api_key, q)

    # Create a CloudStorage Manager to be able to access Google Cloud Storage based on
    # the credentials stored in cs_cred.
    cs = CS.CloudStorage(cs_cred)

    # Create a VertNetHarvestFileProcessor that does the work of improving rows in the 
    # incoming data set.
    processor = VertNetHarvestFileProcessor()

    if harvestfolders is None:
        print 'No harvest folders found matching query %s.' % q
        return None

    print harvestfolders
    # Process harvest folders from GCS and put them back on GCS in the folder "processed".
    process_harvest_folders(cs, harvestfolders, processor)

if __name__ == "__main__":
    main()