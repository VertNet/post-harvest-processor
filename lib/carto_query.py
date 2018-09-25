#!/usr/bin/env python

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

__author__ = "Javier Otegui"
__contributors__ = "Javier Otegui, John Wieczorek"
__copyright__ = "Copyright 2016 vertnet.org"
__this_file__ = "carto_query.py"
__revision_date__ = "2018-09-25T20:53-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

from carto_utils import carto_query
import argparse

def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser()

    help = 'API key for Carto; required)'
    parser.add_argument("-c","--carto_api_key", help=help, required=True)

    return parser.parse_args()

def main():
    """ Example script to send SQL command to Carto."""
    '''
    Send SQL request to Carto.

    Invoke with Carto API key parameter as:
       python check_harvest_folder_GCS.py /
         -c [carto_api_key]
    '''
    url = "https://vertnet.carto.com/api/v2/sql"
    options = _getoptions()

    if options.carto_api_key is None or len(options.carto_api_key)==0:
        s =  'syntax:\n'
        s += 'python %s ' % __this_file__
        s += '-c [carto_api_key '
        print s
        return

    q =  "SELECT orgcountry, count(*) as reps "
    q += "FROM resource_staging "
    q += "WHERE "
    q += "ipt=True AND networks like '%VertNet%' "
    q += "GROUP BY orgcountry "
    q += "ORDER BY reps DESC "

#     q =  "SELECT icode, gbifdatasetid, harvestfolder "
#     q += "FROM resource_staging "
#     q += "WHERE "
#     q += "ipt=True AND networks like '%VertNet%' AND "
#     q += "harvestfolder LIKE 'vertnet-harvesting/data/2018-09-21/%' "
#     q += "order by icode, github_reponame asc"
#     
    result = carto_query(url, options.carto_api_key, q)
    print result

if __name__ == "__main__":
    main()
