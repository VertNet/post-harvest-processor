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
__contributors__ = "Javier Otegui, John Wieczorek"
__copyright__ = "Copyright 2018 vertnet.org"
__this_file__ = "check_harvest_folder_GCS.py"
__revision_date__ = "2018-09-25T14:13-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

# googleapis has an argparser, which will be invoked when the CloudStorage instance is
# initiated. We must use that argparser and add our argument processing to it.
from googleapis.GoogleAPI import parser
from googleapis import CloudStorage as CS
from google_creds import cs_cred
from carto_utils import carto_query
import csv

def check_harvest_folder_GCS(cs, folders):
    '''
       Check that the harvest folders on the list coming out of Carto exist, and how
       many files are in each.
    '''
    if cs is None:
        print 'No Google Cloud Storage dispatcher given in %s.' % __version__
    if folders is None:
        print 'No folder to check in %s.' % __version__
    total = 0
    i = 0
    for f in folders:
        j = 0
        if f.has_key('harvestfolder') and len(f['harvestfolder'].strip()) > 0:
            bucket = f['harvestfolder'].split('/', 1)[0]
            resource = f['harvestfolder'].split('/', 1)[1]

            # Make a list of files in the harvest folder
            uri_list = []
            if 'items' in cs.list_bucket(prefix=resource):
                for item in cs.list_bucket(prefix=resource)['items']:
                    uri = '/'.join(["gs:/", bucket, item['name']])
                    uri_list.append(uri)
                    j += 1
            else:
                # Fail unless all folders can be found. 
                s = 'Resource %s not found in %s. ' % (resource, bucket)
                s += 'Check harvestfolder value in Carto.'
                print '%s' % s
                return False
        i += 1
        total += j
        print '%s) %s files for %s %s %s' % (i, j, f['icode'], f['gbifdatasetid'], f['harvestfolder'])
    print 'Total shards: %s' % total
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

    if harvestfolders is None:
        s = '
        return None

    # Do a preliminary check of the folders in the harvest list
    return check_harvest_folder_GCS(cs, harvestfolders)

if __name__ == "__main__":
    main()