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

# This file contains common utility functions for pulling a list of harvest folders
# out of a resrouce_staging export of the VertNet resource registry in CartoDB

__author__ = "John Wieczorek"
__copyright__ = "Copyright 2018 vertnet.org"
__this_file__ = "carto_utils.py"
__revision_date__ = "2018-09-24T21:36-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

import json
from urllib2 import urlopen
from urllib import urlencode

def carto_query(url, carto_api_key, q):
    '''Send a query to Carto using the REST API.
    url - Carto API url for the account on which the query is to be run
    carto_api_key - API key for the Carto account on which the query is to be run
    q - SQL of the query to run

    returns a list of resulting rows as dictionaries with column_name:value pairs
    '''
    url = "https://vertnet.carto.com/api/v2/sql"
    params = {'api_key':carto_api_key, 'q':q}
    data = urlencode(params)
    raw = urlopen(url, data=data).read()
    result = json.loads(raw)['rows']
    return result
