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
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2017 vertnet.org"
__version__ = "bigquery_loader_stats.py 2017-11-19T11:13-03:00"

from googleapis import CloudStorage as CS
from googleapis import BigQuery as BQ
from google.cloud import bigquery
from creds.google_creds import cs_log_cred
from creds.google_creds import bq_cred
from field_utils import LOG_SCHEMA
from datetime import datetime
import time

BQ_LOG_DATASET='logs'
BQ_LOG_TABLE='vertnet_usage'
BQ_LOG_SCHEMA=LOG_SCHEMA

GCS_FOLDER='vertnet-usage'

def remove_GCS_files(cs, filelist):
    '''
       Remove a list of files from GCS, where the filenames in the list are the paths
       within the bucket defined in cs
       (e.g., processed/YPM/9643f840-f762-11e1-a439-00145eb45e9a/2016-07-16-aa)
    '''
    for file in filelist:
        try:
            cs.delete_object(file)
        except Exception, e:
            print 'Failed to remove file %s Exception: %s' % (file, e)

def get_processed_files(cs, folder):
    '''
       Get the list of files in a folder of processed files.
    '''
    resource = GCS_FOLDER
    folderlist = {}
    listing = []
    dataset = '%s/%s' % (resource, folder)
    bucketlist = cs.list_bucket(prefix=resource+'/'+folder+'/')
    if 'items' in bucketlist:
        for item in bucketlist['items']:
            uri = '/'.join(["gs:/", cs.bucket(), item['name']])
            parts = item['name'].split('/')
            if len(parts) == 3 and len(parts[2])!=0:
                listing.append(uri)
    folderlist[dataset] = { 'dataset': dataset, 'listing': listing }
    return folderlist

def get_all_processed_folders(cs):
    '''
       Get the list of folders of files to copy into BigQuery.
    '''
    resource = GCS_FOLDER
    foldercount = 0
    filecount = 0
    token = None

    # Make a list of folders in resource
    datasetlist = []
    folderlist = {}
    uri_list = []
    thereismore = True
    while filecount==0 or token is not None:
        bucketlist = cs.list_bucket(prefix=resource, pageToken=token)
        if 'items' in bucketlist:
            for item in bucketlist['items']:
                uri = '/'.join(["gs:/", cs.bucket(), item['name']])
#                print 'name: %s' % item['name']
                parts = item['name'].split('/')
                if len(parts) == 3 and len(parts[2])!=0:
                    dataset = '/'.join([parts[0],parts[1]])
                    if dataset not in datasetlist:
                        datasetlist.append(dataset)
                        foldercount += 1
                        listing = []
                        listing.append(uri)
                        folderlist[dataset] = { 'dataset': dataset, 'listing': listing }
                    else:
                        folderlist[dataset]['listing'].append(uri)
                filecount += 1
            if 'nextPageToken' in bucketlist:
                token = bucketlist['nextPageToken']
            else:
                token = None
    return folderlist

def sorted_list_from_dict(thedict):
    '''
       Return the keys for a dict in a sorted list.
    '''
    thelist = []
    for key in thedict:
        thelist.append(key)
    thelist.sort()
    return thelist

def multiple_runs_list(folderdict):
    '''
       Get the list of folders that have more than one set of processed files (more than
       one file for the data set ending in '-aa').
    '''
    datasetlist = sorted_list_from_dict(folderdict)
    dupeslist = []
    for dataset in datasetlist:
        j = 0
        for file in folderdict[dataset]['listing']:
            if file.rfind('-aa') == len(file)-3:
                j += 1
        if j>1:
            dupeslist.append(dataset)

    cleanuplist = []
    for dataset in dupeslist:
        j = 0
        patterns = set([])
        for file in folderdict[dataset]['listing']:
            if file.rfind('-aa') == len(file)-3:
                j += 1
                patterns.add(file[0:len(file)-3])
        newpatterns = list(patterns)
        newpatterns.sort()
        cleanuplist.append(newpatterns[0])
    return cleanuplist

def matching_file_list(folderdict, pattern):
    filelist = []
    datasetlist = sorted_list_from_dict(folderdict)
    for dataset in datasetlist:
        for file in folderdict[dataset]['listing']:
            if pattern in file:
                shard = file[len('gs://vertnet-harvesting/'):len(file)]
                filelist.append(shard)
    return filelist

def load_folders_in_bigquery(cs, folderdict):
    '''
       Load data from all shards in folderdict into a data set in BigQuery.
    '''
    dataset_name=BQ_LOG_DATASET
    dataset_table_name=BQ_LOG_TABLE
    dataset_table_schema=BQ_LOG_SCHEMA

    # Create the dataset if it doesn't exist
    client = bigquery.Client(project=bq_cred['project_id'])

#    bq = BQ.BigQuery(bq_cred)
    existing_dataset_ids=[]
    for dataset in client.list_datasets():
        existing_dataset_ids.append(dataset.dataset_id)
        print 'data set ID: %s' % dataset.dataset_id

    dataset_ref = client.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    if dataset_name not in existing_dataset_ids:
        print 'Creating dataset %s' % dataset_name
        dataset.description = 'Data set container to hold log tables'
        dataset = client.create_dataset(dataset) # API request
    
    # Create the table if it does not exist
    existing_table_ids=[]
    for table in client.list_dataset_tables(dataset):
        existing_table_ids.append(table.table_id)
        print 'Table ID: %s' % table.table_id

#     fields = dataset_table_schema['fields']
#     for field in fields:
#         print 'name: %s type: %s' % (field['name'], field['type'])
#         print '%s' % bigquery.SchemaField(field['name'], field['type'])
#         table.schema.append(bigquery.SchemaField(field['name'], field['type']))
#         a.append(bigquery.SchemaField(field['name'], field['type']))
#         print 'Table schema: %s' % table.schema
#         print 'a: %s' % a

    table_ref = dataset.table(dataset_table_name)
    try:
        table = client.get_table(table_ref)
    except:
        print 'Table %s not found in dataset %s' % (dataset_table_name, dataset_name)
        print 'Creating table %s in dataset %s' % (dataset_table_name, dataset_name)
        table = bigquery.Table(table_ref)
        table.schema=LOG_SCHEMA
        try:
            client.create_table(table)  # API request
        except:
            print 'Unable to create table %s in dataset %s' % (dataset_table_name, dataset_name)

    print 'schema for %s:\n%s' % (BQ_LOG_TABLE,table.schema)

    # Launch a load job for a file
    uri = 'gs://vertnet-logs/vertnet_usage.csv'
    print 'Loading: %s' % uri
    # Prepare the job to load from one uri
    job_id_prefix = "log_table_loader_job"
    job_config = bigquery.LoadJobConfig()
    job_config.create_disposition = 'NEVER'
    job_config.skip_leading_rows = 1
    job_config.source_format = 'CSV'
    job_config.write_disposition = 'WRITE_APPEND'

    load_job = client.load_table_from_uri(
                uri, table_ref, job_config=job_config,
                job_id_prefix=job_id_prefix)  # API request

#            assert load_job.state == 'RUNNING'
#            assert load_job.job_type == 'load'

    load_result=load_job.result()  # Waits for table load to complete.
    assert load_job.state == 'DONE'
    assert load_job.job_id.startswith(job_id_prefix)
    
    # Launch a load job for each folder
#     for folder in folderdict:
#         uri_list = folderdict[folder]['listing']
#         for uri in uri_list:
#             print 'Loading: %s' % uri
#             # Prepare the job to load from one uri
#             job_id_prefix = "log_table_loader_job"
#             job_config = bigquery.LoadJobConfig()
#             job_config.create_disposition = 'NEVER'
#             job_config.skip_leading_rows = 1
#             job_config.source_format = 'CSV'
#             job_config.write_disposition = 'WRITE_APPEND'
# 
#             load_job = client.load_table_from_uri(
#                 uri, table_ref, job_config=job_config,
#                 job_id_prefix=job_id_prefix)  # API request
# 
# #            assert load_job.state == 'RUNNING'
# #            assert load_job.job_type == 'load'
# 
#             load_result=load_job.result()  # Waits for table load to complete.
# 
#             assert load_job.state == 'DONE'
#             assert load_job.job_id.startswith(job_id_prefix)

    return

def load_folders_in_bigquery_old(cs, folderdict):
    '''
       Load data from all shards in folderdict into a data set in BigQuery.
    '''
    # Load BigQuery table schema from file
    schema = get_schema()
    if schema is None:
        return

    # BigQuery naming
    dataset_name = BQ_LOG_DATASET
    table_name = BQ_LOG_TABLE
    load_jobs = {}

#    print 'BigQuery table schema for %s.%s:\n%s' % (dataset_name, table_name, schema)
#    print 'folderdict: %s' % folderdict

    # Create the dataset if it doesn't exist
    bq = BQ.BigQuery(bq_cred)
    print 'Creating dataset %s' % dataset_name
    bq.create_dataset(dataset_name)

    # Launch a load job for each folder
    for folder in folderdict:
#       if folder in ['processed/ROM/e0dbf705-cec4-4dce-a152-fc4ebe14674d', \
#         'processed/KU/8f79c802-a58c-447f-99aa-1d6a0790825a', \
#         'processed/FMNH/36f15a36-6b45-442e-9c31-cd633423aee0']:
        print 'Loading: %s' % folder
        uri_list = folderdict[folder]['listing']
        # Launch a job for loading all the chunks in a single folder
        job_id = bq.create_load_job(ds_name=dataset_name,
                                    table_name=table_name,
                                    uri_list=uri_list,
                                    schema=schema)
        
        # Store the job_id in the dictionary of job_ids
        load_jobs[folder] = job_id
        time.sleep(1)

    # Wait until all jobs finish
    bq.wait_jobs(10)

    # Check failed jobs
    failed_resources = check_failed(bq, load_jobs)
    
    if len(failed_resources) > 0:
        dump_file = './data/failed.txt'
        print "Some chunks could not be loaded into BigQuery."
        with open(dump_file, 'w') as w:
            for i in failed_resources:
                w.write(i)
                w.write("\n")
        print "These have been written to {0}".format(dump_file)
    
    # The end
    return

def build_uri_list(cs, resource):
    """Build a list containing the URIs of the shards of a single resource."""
    
    print "Building list of chunks for {0}".format(resource)
    
    uri_list = []
    for i in cs.list_bucket(prefix=resource)['items']:
        uri = '/'.join(["gs:/", cs._BUCKET_NAME, i['name']])
        uri_list.append(uri)
    
    return uri_list

def check_failed(bq, load_jobs):
	# Check failed jobs
    # Reset the failed_resources list
    failed_resources = []
    for resource in load_jobs:
        job_id = load_jobs[resource]['jobReference']['jobId']
        status = bq.check_job(job_id)
        print 'job status: %s' % status
        if status['state'] == 'DONE' and 'errorResult' in status:
            print 'Resource {0} failed and job was aborted. Queued for individual load.'
            failed_resources.append(resource)
    return failed_resources

def launch_load_job(uri_list):
    """Launch a job for loading all the chunks in a single resource."""
    print "Launching load job"
    return job_id

def main():
    '''
    This script analyzes the folders in the Google Cloud vertnet-portal/vertnet-logs and 
    then removes up to one older duplicate. If no duplicates are found, the script uses 
    contents of the vertnet-portal/processing folder to populate a BigQuery table 
    dumps/full_yyyymmdd. If there are no duplicate folders, expect the script to run to
    completion in 10 minutes or less.
    This script analyzes the folders in the Google Cloud bucket 
    vertnet-portal/vertnet-logs. The script uses contents of those folders to populate a 
    BigQuery table logs/vertnet. 
    '''
    
    # Create a CloudStorage Manager to be able to access Google Cloud Storage based on
    # the credentials stored in cs_cred.
    cs = CS.CloudStorage(cs_log_cred)

    # Create a dict of processed folders on Google Cloud Storage to copy into BigQuery
    # Check for folders that have multiple harvest dates in them and clean out old 
    # versions before loading BigQuery
    processedfolders = get_all_processed_folders(cs)

    if processedfolders is None or len(processedfolders) == 0:
        print 'No processed folders found'
        return False
    else:
        print '%s processed folders:' % len(processedfolders)
        for file in processedfolders:
            print '%s' % file
           
    # Or create a subset to process based on a particular folder in GCS. 
#    processedfolders2 = get_processed_files(cs, '2016')
#    print 'processedfolders2: %s' % processedfolders2

    load_folders_in_bigquery(cs, processedfolders)

if __name__ == "__main__":
    main()