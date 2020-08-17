from pysgcn import sgcn as pysgcn
import math
import json
import hashlib

import sys
import requests

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("./validation_output.txt", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)  

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass    

def get_total_records_processed_by_pipeline(pipeline_run):
    print('Getting records processed by SGCN pipeline...')
    URL = "https://7y9ycz4ki4.execute-api.us-west-2.amazonaws.com/prod/runs/" + pipeline_run

    r = requests.get(url = URL) 
  
    data = r.json() 
    return data['data']['documents_ingested']


def get_total_input_items():
    
    print('Processing all SWAP files...')
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager="foo")
    sgcn_meta = sgcn.cache_sgcn_metadata(return_data=True)
    items = sgcn.get_processable_items()
    
    total_species_ct = 0
    bad_record_ct = 0
    dupe_record_ct = 0
    state_ct = 0
    
    for item in items:
        print('--> state: {} ({})'.format(item['state'], item['year']))
        state_ct = state_ct + 1
        res = sgcn.process_sgcn_source_item(item, metadata_cache=sgcn_meta)
        species_ct = 0
        bad_state_ct = 0
        dupe_state_ct = 0
        species_set = set()
        for species in res:
            # Stuff that can be uncommented if we need to debug deeper into missing/invalid records.
            #if item['state'] == "West Virginia" and item['year'] == "2015":
            #    print('{}:{}:{}'.format(item['state'], item['year'], species['scientific name']))
                #if species['scientific name'].lower() == "artibeus jamaicensis" :
                #    print('{}'.format(json.dumps(species)))
            valid = sgcn.validate_data(species)
            # create a hash of the species record so we don't add duplicates from the same file
            hsh = hashlib.sha1(repr(json.dumps(species, sort_keys=True)).encode('utf-8')).hexdigest()
            
            if not isinstance(species['scientific name'], float) and "no scientific name" in species['scientific name'].lower():
                print('    Potential Bad Record : {}'.format({species['scientific name'], species['common name']}))

            # check for duplicates
            if valid and hsh not in species_set:
                species_set.add(hsh)
            elif hsh in species_set:
                print('    Duplicate Record: {}'.format({species['scientific name'], species['common name']}))
                dupe_state_ct = dupe_state_ct + 1
            elif not valid:
                print('    Bad Record : {}'.format({species['scientific name'], species['common name']}))
                bad_state_ct = bad_state_ct + 1  
                
            species_ct = species_ct + 1
            
        print('    Species Ct: {}'.format(species_ct - bad_state_ct - dupe_state_ct))
        dupe_record_ct = dupe_record_ct + dupe_state_ct
        bad_record_ct = bad_record_ct + bad_state_ct
        total_species_ct = total_species_ct + species_ct
        
     
    print('\ntotal states processed = {}'.format(state_ct))
    print('total species ct = {}'.format(total_species_ct))
    print('total bad record ct = {}'.format(bad_record_ct))
    print('total dupe record ct = {}'.format(dupe_record_ct))
    print('final species ct = {}'.format(total_species_ct - bad_record_ct - dupe_record_ct))
 
sys.stdout = Logger() 

# CHANGE THIS PARAMETER to be the run id of your latest SGCN pipeline run
pipeline_run = "705da83c-de64-11ea-a3a1-023f40fa784e"

total_processed = get_total_records_processed_by_pipeline(pipeline_run)    

get_total_input_items()
print('\ntotal SGCN pipeline records = {}'.format(total_processed))  



