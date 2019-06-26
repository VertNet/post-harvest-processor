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

__author__ = "John Wieczorek"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2018 vertnet.org"
__this_file__ = "bigquery_query.py"
__revision_date__ = "2018-09-25T21:24-03:00"
__version__ = "%s %s" % (__this_file__, __revision_date__)

'''
Command-line application that demonstrates basic BigQuery API usage.

This sample is adapted from this page:
    https://cloud.google.com/bigquery/bigquery-api-quickstart

For more information, see the README.md under /bigquery.
'''
import argparse
import copy

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

def stats_query(bigquery_service, project_id):
    try:
        # Make a query to get list of distinct collections with count of records
        query_request = bigquery_service.jobs()
        select = 'SELECT * '
        select += 'FROM [vertnet-portal.logs.query_log_master2018_07_13] '
        select += 'LIMIT 10;'

        query_data = {
            'query': (select)
        }

        # Send the query request to BigQuery
        query_response = query_request.query(
            projectId=project_id,
            body=query_data).execute()

    except HttpError as err:
        print('Error: {}'.format(err.content))
        raise err

    print 'stats response: %s' % (query_response)
#    print 'stats response: %s' % (query_response['rows'])

def habitat_query(bigquery_service, project_id):
    habitats = ['sand', 'ocean', 'forest', 'bed', 'rock', 'river', 'lake', 'gravel', 'pond', 'stream']
    for habitat in habitats:
        try:
            query_request = bigquery_service.jobs()
            select = 'SELECT count(*) as records '
            select += 'FROM [vertnet-portal:dumps.vertnet_latest] '
            select += 'WHERE '
            select += 'lower(occurrenceremarks) LIKE "%%%s%%" AND ' % habitat
            select += 'lower(habitat) NOT LIKE "%%%s%%"' % habitat
            select += ';'

            query_data = {
                'query': (select)
            }

            query_response = query_request.query(
                projectId=project_id,
                body=query_data).execute()
        
        except HttpError as err:
            print('Error: {}'.format(err.content))
            raise err

        print 'habitat: %s response: %s' % (habitat, query_response['rows'])

def georefs_query(bigquery_service, project_id):
    try:
        # Make a query to get list of distinct collections with count of records
        query_request = bigquery_service.jobs()
        select = 'SELECT icode, collectioncode, count(*) as records '
        select += 'FROM [vertnet-portal:dumps.vertnet_latest] '
        select += 'GROUP BY icode, collectioncode '
        select += 'ORDER BY icode, collectioncode;'

        query_data = {
            'query': (select)
        }

        # Send the query request to BigQuery
        query_response = query_request.query(
            projectId=project_id,
            body=query_data).execute()

        # Construct a list of icodes and a stats dictionary for each of them
        icodes = []
        stats = {}
        for row in query_response['rows']:
            inst = row['f'][0]['v']
            coll = row['f'][1]['v']
            count = int(row['f'][2]['v'])
            if coll is None:
                instcoll = inst
            else:
                instcoll = '%s %s' % (inst, coll)
            icodes.append(instcoll)
            icode = []
            icode.append(count)
            stats[instcoll] = icode

        # Make a query to get list of collections with count of records with coordinates
        query_request = bigquery_service.jobs()
        select = 'SELECT icode, collectioncode, count(*) as georeferences '
        select += 'FROM [vertnet-portal:dumps.vertnet_latest] '
        select += 'WHERE mappable="1" '
        select += 'GROUP BY icode, collectioncode '
        select += 'ORDER BY icode, collectioncode;'

        query_data = {
            'query': (select)
        }

        # Send the query request to BigQuery
        query_response = query_request.query(
            projectId=project_id,
            body=query_data).execute()

        # ???
        for row in query_response['rows']:
            inst = row['f'][0]['v']
            coll = row['f'][1]['v']
            count = int(row['f'][2]['v'])
            if coll is None:
                instcoll = inst
            else:
                instcoll = '%s %s' % (inst, coll)
            stats[instcoll].append(count)

        print('Query Results:')
        print('collection\trecords\tgeorefs')
        for icode in icodes:
            if len(stats[icode]) > 1:
                stat = '\t'.join(str(field) for field in stats[icode])
            else:
                stat = '%s\t0' % stats[icode][0]
            print '%s\t%s' % (icode, stat)

    except HttpError as err:
        print('Error: {}'.format(err.content))
        raise err

def _getoptions():
    ''' Parse command line options and return them.'''
    parser = argparse.ArgumentParser( description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    help = 'Google Cloud Project Number; required)'
    parser.add_argument("-p","--project_id", help=help, required=True)

    return parser.parse_args()

def main(project_id):
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()
    # Construct the service object for interacting with the BigQuery API.
    bigquery_service = build('bigquery', 'v2', credentials=credentials)

    stats_query(bigquery_service, project_id)
    # habitat_query(bigquery_service, project_id)
    # georefs_query(bigquery_service, project_id)

if __name__ == '__main__':
    options = _getoptions()
    if options.project_id is None or len(options.project_id)==0:
        s =  'syntax:\n'
        s += 'python %s ' % __this_file__
        s += '-p vertnet-portal'
        print s
    else:
        main(options.project_id)
