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
__revision_date__ = "2018-09-24T20:59-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

"""Service to generate stats for the stats page."""

import carto_utils
from urllib2 import urlopen
from urllib import urlencode
import json

def main():
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'cdbkey.txt')
    api_key = open(path, "r").read().rstrip()
    print "CARTO KEY %s" % api_key
    logging.info("CARTO KEY %s" % api_key)

    url = "https://vertnet.carto.com/api/v2/sql"
    q =  "SELECT icode, gbifdatasetid, harvestfolder "
    q += "FROM resource_staging "
    q += "WHERE "
    q += "ipt=True AND networks like '%VertNet%' AND "
    q += "harvestfolder LIKE 'vertnet-harvesting/data/2018-09-21/%' "
    q += "order by icode, github_reponame asc"
    
    params = {'api_key':api_key, 'q':q}
    data = urlencode(params)

    raw = urlopen(url, data=data).read()
    d = json.loads(raw)['rows']
    print "d: %s" % d

if __name__ == "__main__":
    main()
