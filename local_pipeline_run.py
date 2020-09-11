import json
from dotenv import load_dotenv, find_dotenv
from pysgcn import bis_pipeline
import pysppin
import time
import sys

load_dotenv(find_dotenv())

ch_ledger = 'ledger'
cache_root = 'mydatabase'

def lambda_handler_4(event, context):
    message_in = json.loads(event["body"])
    run_id = message_in["run_id"]
    sb_item_id = message_in["sb_item_id"]
    download_uri = message_in["download_uri"]
    cache_manager = CacheManager(download_uri)

    send_final_result = None
    send_to_stage = None

    bis_pipeline.process_4(download_uri, ch_ledger, send_final_result, send_to_stage, message_in["payload"], cache_manager)

def lambda_handler_3(event, context):
    message_in = json.loads(event["body"])
    run_id = message_in["run_id"]
    sb_item_id = message_in["sb_item_id"]
    download_uri = message_in["download_uri"]
    cache_manager = CacheManager(download_uri)

    def send_to_stage(data, stage):
        json_doc = {
            'run_id': run_id,
            'sb_item_id': sb_item_id,
            'download_uri': download_uri,
            'payload': data
        }
        lambda_handler_4({"body": json.dumps(json_doc)}, {})

    def send_final_result(data):
        species = data["data"]
        row_id = data["row_id"]
        cache_manager.add_to_cache("final_res:{}".format(species["sppin_key"]), species)
        # cache_manager.add_to_cache(row_id, species)

    bis_pipeline.process_3(download_uri, ch_ledger, send_final_result, send_to_stage, message_in["payload"], cache_manager)

def lambda_handler_2(event, context):
    message_in = json.loads(event["body"])
    run_id = message_in["run_id"]
    sb_item_id = message_in["sb_item_id"]
    download_uri = message_in["download_uri"]
    cache_manager = CacheManager(download_uri)

    def send_to_stage(data, stage):
        json_doc = {
            'run_id': run_id,
            'sb_item_id': sb_item_id,
            'download_uri': download_uri,
            'payload': data
        }
        lambda_handler_3({"body": json.dumps(json_doc)}, {})

    send_final_result = None

    start_time = time.time()
    num_species = bis_pipeline.process_2(download_uri, ch_ledger, send_final_result, send_to_stage, message_in["payload"], cache_manager)
    elapsed_time = "{:.2f}".format(time.time() - start_time)
    print('Species count: {} ({} seconds)'.format(num_species, elapsed_time))

def lambda_handler(event, context):
    run_id = event["run_id"]
    sb_item_id = event["sb_item_id"]
    download_uri = event["download_uri"]
    cache_manager = CacheManager(download_uri)

    def send_to_stage(data, stage):
        json_doc = {
            'run_id': run_id,
            'sb_item_id': sb_item_id,
            'download_uri': download_uri,
            'payload': data
        }
        lambda_handler_2({"body": json.dumps(json_doc)}, {})

    send_final_result = None

    num_process_files = bis_pipeline.process_1(download_uri, ch_ledger, send_final_result, send_to_stage, sb_item_id, cache_manager)

class CacheManager:
    def __init__(self, cache_root):
        self.cache_folder = "sppin"
        self.cache_path = f"{cache_root}/{self.cache_folder}"
        self.sql_cache = pysppin.utils.Sql(cache_location=self.cache_path)
        self.table_name = 'cache'
    
    def get_from_cache(self, key):
        res = self.sql_cache.get_select_records(self.cache_folder, self.table_name, 'key = ?', key)
        return res[0]["value"] if res else None

    def add_to_cache(self, key, value):

        res = self.get_from_cache(key)
        if res:
            return res;

        data = {"key": key, "value": value}
        return self.sql_cache.insert_record(self.cache_folder, self.table_name, data)

class Logger(object):
    def __init__(self):
        self.terminal = sys.stdout
        self.log = open("./pipeline_output.txt", "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass

sys.stdout = Logger()
lambda_handler({
    "run_id": "705da83c-de64-11ea-a3a1-023f40fa784e",
    # This item_id gives all 112 state/year combos to process
    "sb_item_id": "56d720ece4b015c306f442d5",

    # This item_id is our test location that gives just a few state/year combos
    #"sb_item_id": "5ef51d8082ced62aaae69f05",  OBSOLETE, Don't use.
    "download_uri": cache_root
}, {})