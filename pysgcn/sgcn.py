from sciencebasepy import SbSession
import pandas as pd
import requests
from datetime import datetime
import pysppin
import os
import json
import pkg_resources

common_utils = pysppin.utils.Utils()
itis_api = pysppin.itis.ItisApi()
worms = pysppin.worms.Worms()
pysppin_utils = pysppin.utils.Utils()


class Sgcn:
    def __init__(self, operation_mode="local", cache_root=None):
        self.description = "Set of functions for assembling the SGCN database"
        self.sgcn_root_item = '56d720ece4b015c306f442d5'
        self.resources_path = 'resources/'

        self.sb = SbSession()
        self.sgcn_base_item = self.sb.get_item(self.sgcn_root_item)

        self.historic_national_list_file = next(
            (f["url"] for f in self.sgcn_base_item["files"] if f["title"] == "Historic 2005 SWAP National List"), None)
        self.sgcn_itis_overrides_file = next((f["url"] for f in self.sgcn_base_item["files"]
                                              if f["title"] == "SGCN ITIS Overrides"), None)

        self.sppin_collections = [
            "itis",
            "worms",
            "gbif",
            "ecos",
            "natureserve",
            "iucn",
            "gap"
        ]

        if operation_mode == "local":
            self.source_data_folder = "sgcn"
            self.source_metadata_folder = "sgnc_meta"
            self.mq_folder = "mq"
            self.sppin_folder = "sppin"
            self.raw_data_folder = "raw"

            if cache_root is None:
                if os.getenv("DATA_CACHE") is None:
                    raise ValueError("When operating this system locally, you must either supply an explicit cache_"
                                     "location to a local path or include the DATA_CACHE variable in your environment "
                                     "variables.")
                else:
                    self.cache_base = f'{os.getenv("DATA_CACHE")}'
            else:
                self.cache_base = cache_root

            self.source_data_path = f"{self.cache_base}/{self.source_data_folder}"
            self.source_metadata_path = f"{self.cache_base}/{self.source_metadata_folder}"
            self.mq_path = f"{self.cache_base}/{self.mq_folder}"
            self.sppin_path = f"{self.cache_base}/{self.sppin_folder}"
            self.raw_data_path = f"{self.cache_base}/{self.raw_data_folder}"

            try:
                os.makedirs(self.cache_base)
            except FileExistsError:
                pass

            try:
                os.makedirs(self.source_data_path)
            except FileExistsError:
                pass

            try:
                os.makedirs(self.source_metadata_path)
            except FileExistsError:
                pass

            try:
                os.makedirs(self.mq_path)
            except FileExistsError:
                pass

            try:
                os.makedirs(self.mq_path)
            except FileExistsError:
                pass

            try:
                os.makedirs(self.raw_data_path)
            except FileExistsError:
                pass

            self.sql_metadata = pysppin.utils.Sql(cache_location=self.source_metadata_path)
            self.sql_data = pysppin.utils.Sql(cache_location=self.source_data_path)
            self.sql_mq = pysppin.utils.Sql(cache_location=self.mq_path)
            self.sql_sppin = pysppin.utils.Sql(cache_location=self.sppin_path)

    def cache_sgcn_metadata(self, return_data=False):
        '''
        The SGCN collection item contains a number of metadata files that help to control and augment the process of
        building the SGCN integrated database. For running this process locally, it is more efficient to cache these
        data in a Sqlite database that can be referenced rather than having to retrieve them from ScienceBase every
        time they need to be consulted.

        :param return_data: Set to true to return the actual data structures instead of just a list of tables
        :return: List of table names created in caching process
        '''
        sgcn_collection = self.sb.get_item(self.sgcn_root_item)

        if return_data:
            table_list = dict()
        else:
            table_list = list()

        for file in sgcn_collection["files"]:
            r_file = requests.get(file["url"])
            if file["contentType"] == "text/plain":
                data_content = list()
                for item in r_file.text.split("\n"):
                    data_content.append({
                        "scientific_name": item
                    })
            else:
                data_content = r_file.json()

            if return_data:
                table_list[file["title"]] = data_content

            try:
                self.sql_metadata.bulk_insert("sgcn_meta", file["title"], data_content)
                if not return_data:
                    table_list.append(file["title"])
            except:
                if not return_data:
                    table_list.append(f'{file["title"]} - ALREADY CACHED')

        return table_list

    def check_historic_list(self, scientific_name):
        '''
        This function takes a scientific name and checks to see if it was included in the 2005 SWAP list

        :param scientificname: Scientific name string
        :return: True if the name is in the historic list, otherwise False
        '''
        check_records = self.sql_metadata.get_select_records(
            "sgcn_meta",
            "Historic 2005 SWAP National List",
            "scientific_name = ?",
            scientific_name
        )

        if check_records is None:
            return False
        else:
            return True

    def check_itis_override(self, scientific_name):
        '''
        This function takes the original scientific name found in certain source records and finds a corresponding
        ITIS identifier to be used in lieu of name lookup.

        :param scientific_name: Scientific name string
        :return: ITIS TSN identifier in URL form
        '''
        check_records = self.sql_metadata.get_select_records(
            "sgcn_meta",
            "SGCN ITIS Overrides",
            "ScientificName_original = ?",
            scientific_name
        )

        if check_records is None:
            return None

        return check_records[0]["taxonomicAuthorityID"]

    def cache_raw_data(self):
        '''
        After having some trouble crop up occasionally where reading files from ScienceBase came up with a urlopen
        error, this function takes another approach of simply trying to get all files and download them to a local
        cache.

        :return: List of files cached
        '''

        processable_items = self.get_processable_items()
        report = {
            "files_written": list(),
            "files_in_cache": list(),
            "file_download_errors": list()
        }

        for item in processable_items:
            file_name = item["source_file_url"].split("%2F")[-1]
            file_path = f"{self.raw_data_path}/{file_name}"

            if os.path.isfile(file_path):
                report["files_in_cache"].append(file_path)
            else:
                try:
                    item_file_content = requests.get(item["source_file_url"])
                    report["files_written"].append(file_path)
                    with open(file_path, "w") as f:
                        f.write(item_file_content.text)
                        f.close()
                except:
                    report["file_download_errors"].append(item["sciencebase_item_id"])
                    pass

        return report

    def get_processable_items(self):
        '''
        Retrieves the items from the ScienceBase collection that have the necessary parameters for processing. It
        checks a process log (not yet in place) to determine whether or not the Process File has already been processed.

        :return: Summarized list of items with just the properties necessary to run the process
        '''
        params = {
            "parentId": self.sgcn_root_item,
            "fields": "title,dates,files,tags",
            "max": 1000
        }

        items = self.sb.find_items(params)

        source_items = list()
        while items and 'items' in items:
            source_items.extend(items["items"])
            items = self.sb.next(items)

        processable_sgcn_items = [
            {
                "sciencebase_item_id": i["link"]["url"],
                "state": next(t["name"] for t in i["tags"] if t["type"] == "Place"),
                "year": next(d["dateString"] for d in i["dates"] if d["type"] == "Collected"),
                "source_file_url": next(f["url"] for f in i["files"] if f["title"] == "Process File"),
                "source_file_date": next(f["dateUploaded"] for f in i["files"] if f["title"] == "Process File")
            }
            for i in source_items
            if next((f for f in i["files"] if f["title"] == "Process File"), None) is not None
        ]

        unprocessed_items = [i for i in processable_sgcn_items if self.check_source_url(i["source_file_url"]) is None]

        return unprocessed_items

    def get_schema(self, schema):
        schema_file = pkg_resources.resource_filename('pysgcn', f'resources/{schema}.json')

        with open(schema_file, "r") as f:
            schema = json.load(f)
            f.close()

        return schema

    def check_source_url(self, source_file_url):
        '''
        This is intended to check a processing log that doesn't yet exist to see if an item's file has already been
        processed. It should shift to using an API that takes the item and returns an existing processing provenance
        record. The parameters used here are the source file URL and the source file date. We could probably get away
        with just using the URL as that should be unique in the ScienceBase architecture today for any new file loaded
        to the items. In future, though, we may have some other source platform that would need to combine a date on a
        file with some other parameter.

        :param item: Simplified SGCN source item dictionary
        :return: Intended to return a process log record if an item has been processed; otherwise returns None
        '''
        return self.sql_data.get_select_records(
            "sgcn",
            "sgcn",
            "source_file_url = ?",
            source_file_url
        )

    def build_sppin_key(self, scientific_name, itis_override_id):
        if itis_override_id is not None:
            return f"TSN:{itis_override_id.split(':')[-1]}"
        else:
            return f"Scientific Name:{scientific_name}"

    def process_sgcn_source_item(self, item, output_type="dict"):
        '''
        This function handles the process of pulling a source file from ScienceBase, reading the specified file via
        HTTP into a Pandas dataframe, infusing a little bit of additional source metadata into each record, infusing
        some additional information from the source collection, and then returning a ready and mostly harmonized
        data structure for further processing.

        :param item: Dictionary containing the summarized item message created and queued in the
        get_processable_items function
        :param output_type: Can be one of - dict, dataframe, or json - defaults to dict
        :return: Returns a flattened data structure/table in one of a few specified formats
        '''
        file_name = item["source_file_url"].split("%2F")[-1]
        file_path = f"{self.raw_data_path}/{file_name}"

        if os.path.isfile(file_path):
            file_access_path = file_path
        else:
            file_access_path = item["source_file_url"]

        try:
            df_src = pd.read_csv(file_access_path, delimiter="\t")
        except UnicodeDecodeError:
            df_src = pd.read_csv(file_access_path, delimiter="\t", encoding='latin1')

        # Make lower case columns to deal with slight variation in source files
        df_src.columns = map(str.lower, df_src.columns)

        # Include the source item identifier
        df_src["sciencebase_item_id"] = item["sciencebase_item_id"]

        # Include a processing date
        df_src["record_processed"] = datetime.utcnow().isoformat()

        # Set the file date and url from the ScienceBase file to each record in the dataset for future reference
        df_src["source_file_date"] = item["source_file_date"]
        df_src["source_file_url"] = item["source_file_url"]

        # Set the state name from the ScienceBase Item tag if needed
        if "state" not in df_src.columns:
            df_src["state"] = item["state"]

        # Set the reporting year from the ScienceBase Item date if needed
        if "year" not in df_src.columns:
            df_src["year"] = item["year"]

        # Get rid of the reported '2005 SWAP' column because we can't count on it and it's too messy
        if "2005 swap" in df_src.columns:
            df_src.drop("2005 swap", axis=1, inplace=True)

        # Standardize naming of the reported taxonomic group column (though we may get rid of this eventually)
        if "taxonomy group" in df_src.columns:
            df_src.rename(columns={"taxonomy group": "taxonomic category"}, inplace=True)

        # Take care of the one weird corner case
        if "taxonomy group (use drop down box)" in df_src.columns:
            df_src.rename(columns={"taxonomy group (use drop down box)": "taxonomic category"}, inplace=True)

        # Clean up the scientific name string for lookup by applying the function from bis_utils
        df_src["clean_scientific_name"] = df_src.apply(
            lambda x: common_utils.clean_scientific_name(x["scientific name"]),
            axis=1)

        # Check the historic list and flag any species names that should be considered part of the 2005 National List
        df_src["historic_list"] = df_src.apply(lambda x: self.check_historic_list(x["scientific name"]), axis=1)

        # Check to see if there is an explicit ITIS identifier that should be applied to the species name (ITIS Overrides)
        df_src["itis_override_id"] = df_src.apply(lambda x: self.check_itis_override(x["scientific name"]), axis=1)

        # Set up the search_key property for use in linking other discovered data from sppin processing
        df_src["sppin_key"] = df_src.apply(
            lambda x: self.build_sppin_key(x["clean_scientific_name"], x["itis_override_id"]), axis=1
        )

        if output_type == "dataframe":
            return df_src
        elif output_type == "dict":
            return df_src.to_dict("records")
        elif output_type == "json":
            return df_src.to_json(orient="records")

    def cache_item_data(self, item, send_record_to_mq=True, send_spp_to_mq=True):
        '''
        This function handles the process of caching (or retrieving from cache if it already exists) a single SGCN
        item's data file. If the file doesn't already exist in the cash, it will fire the process_sgcn_source_item
        function to pull the file from ScienceBase, build it into a dataframe, and then cache as a feather file.

        :param item: Dictionary containing the summarized item message created and queued in the
        get_processable_items function
        :param send_record_to_mq: Send each extracted source record to message queue for further processing
        :param send_spp_to_mq: Extract species names from the source dataset and send to message queue for processing
        :return: Dataset as a Pandas dataframe
        '''
        if self.check_source_url(item["source_file_url"]) is not None:
            raise ValueError("Source file has already been processed and included in the database")

        dataset = self.process_sgcn_source_item(item)

        if send_record_to_mq:
            for record in dataset:
                self.sql_mq.insert_record(
                    db_name="mq",
                    table_name="mq_source_records",
                    record=record,
                    mq=True
                )

        if send_spp_to_mq:
            for msg in self.sppin_messages(dataset=dataset):
                self.sql_mq.insert_record(
                    db_name="mq",
                    table_name="mq_itis_check",
                    record=msg,
                    mq=True
                )

        return dataset

    def sppin_messages(self, dataset=None, scientific_name_list=None, name_source=None):
        '''
        This function extracts the takes either a source dataset (list of dicts) or a list of scientific names and
        packages the necessary message structure for further processing. It uses or creates the sppin_key property
        used throughout data generated with pySppIn methods to link information together.

        :param dataset: Source dataset in dictionary format
        :param scientific_name_list: List of names to assemble
        :param name_source: String value with information on where a list of names comes from
        :return: List of message body dictionary structures containing necessary information for executing lookup
        processes
        '''
        if dataset is None and scientific_name_list is None:
            raise ValueError("You must supply either a dataset (list of dicts) or a list of scientific names")

        if dataset is not None and scientific_name_list is not None:
            raise ValueError("You can only process a dataset (list of dicts) or a list of scientific names, not both")

        mq_list = None

        if dataset is not None:
            mq_list = [{
                "source": {
                    "type": "ScienceBase Source File",
                    "sciencebase_source_item": dataset[0]['sciencebase_item_id'],
                    "sciencebase_source_file": dataset[0]['source_file_url'],
                    "sciencebase_source_file_date": dataset[0]['source_file_date']
                },
                "sppin_key": sppin_key
            } for sppin_key in list(set([i["sppin_key"] for i in dataset]))]

        if scientific_name_list is not None:
            mq_list = [{
                "source": {
                    "type": "List of Scientific Names",
                    "name_source": name_source
                },
                "sppin_key": f"Scientific Name:{name}"
            } for name in list(set([n for n in scientific_name_list]))]

        return mq_list

    def check_sppin_key(self, message_body, sppin_collections=None):
        '''
        Uses the message_body format of a queued scientific name or identifier, checks and parses the sppin_key
        parameter into its parts, and checks a specified set of Species Information containers for records.

        :param message_body: Dictionary containing a queued species identifier
        :param sppin_collections: List of the SppIn collections to search
        :return: Dictionary containing sppin_key, sppin_key type, sppin_key value, and sppin_data from specified
        collections
        '''
        if "sppin_key" not in message_body.keys():
            raise ValueError("The message body must contain the sppin_key parameter")

        sppin_key_parts = message_body["sppin_key"].split(":")

        if len(sppin_key_parts) < 2:
            raise ValueError("Your sppin_key parameter could not be successfully parsed")

        if sppin_collections is None:
            sppin_collections = self.sppin_collections

        sppin_data = dict()
        for collection in sppin_collections:
            sppin_data[collection] = self.sql_sppin.sppin_key_current_record(
                collection,
                message_body["sppin_key"]
            )

        return message_body["sppin_key"], sppin_key_parts[0], sppin_key_parts[1], sppin_data

    def process_itis_result(self, itis_result):
        '''
        This function processes a set of results from ITIS to summarize data for use in SGCN, extract additional names
        for processing through information gathering functions, and set up WoRMS processing if a scientific name
        is not found in ITIS.

        :param itis_result: Dictionary with ITIS data structure returned from the pysppin module
        :return: Dictionary containing ITIS summary properties needed for this application and lists of messages for
        name processing in information gathering functions and WoRMS. Any of these can be None.
        '''
        sppin_key = itis_result["sppin_key"]
        sppin_key_type = itis_result["sppin_key"].split(":")[0]
        sppin_key_value = itis_result["sppin_key"].split(":")[1]

        name_list = list()
        if sppin_key_type == "Scientific Name":
            name_list = [sppin_key_value]
        itis_summary_msg = None
        name_queue = None
        worms_queue = None

        if "data" not in itis_result.keys() or isinstance(itis_result["data"], float):
            worms_queue = self.sppin_messages(
                scientific_name_list=name_list,
                name_source="ITIS Search"
            )

        else:
            name_list.extend([i["nameWInd"] for i in itis_result["data"]])
            name_list.extend([i["nameWOInd"] for i in itis_result["data"]])

            valid_itis_doc = next((i for i in itis_result["data"] if i["usage"] in ["valid", "accepted"]), None)

            if valid_itis_doc is None:
                worms_queue = self.sppin_messages(
                    scientific_name_list=name_list,
                    name_source="ITIS Search"
                )
            else:
                itis_summary_msg = itis_result["summary"]
                itis_summary_msg["sppin_key"] = sppin_key

        if len(name_list) > 0:
            name_queue = self.sppin_messages(
                scientific_name_list=list(set(name_list)),
                name_source="ITIS Search"
            )

        return itis_summary_msg, name_queue, worms_queue

    def process_worms_result(self, worms_result):
        '''
        This function processes a result from the WoRMS search function in pysppin and returns a summary result and
        additional names for processing through other information gathering functions.

        :param worms_result: Dictionary with WoRMS result from pysppin
        :return: Summary properties for processing in SGCN and a list of name messages for further processing
        '''
        sppin_key = worms_result["sppin_key"]
        sppin_key_value = worms_result["sppin_key"].split(":")[1]

        name_list = [sppin_key_value]
        name_queue = None
        worms_summary_msg = None

        if "data" in worms_result.keys():
            name_list.extend([i["scientificname"] for i in worms_result["data"]])

            valid_worms_doc = next((i for i in worms_result["data"] if i["status"] == "accepted"), None)

            if valid_worms_doc is not None:
                worms_summary_msg = worms_result["summary"]
                worms_summary_msg["sppin_key"] = sppin_key

        if len(name_list) > 0:
            name_queue = self.sppin_messages(
                scientific_name_list=list(set(name_list)),
                name_source="WoRMS Search"
            )

        return worms_summary_msg, name_queue

    def process_sppin_source_search_term(self, message_queue, sppin_source, message_id=None, message_body=None):
        '''
        This function operates any of the basic pySppIn gatherers that use the sppin_key parameter to lookup by
        Scientific Name or ITIS TSN. It fires the check_sppin_key function to both parse the sppin_key parameter and
        check the cache for an existing record. That function currently defaults to looking for records in the last
        30 days but this can be configured based on the situation.

        :param message_queue: the name of the message queue to process
        :param sppin_source: the species information source to operate against
        :param message_id: identifier for the message containing the search term (if None, the function will fire the
        get_message function to attempt to retrieve a message from the specified queue to process)
        :param message_body: body of the message containing the search term and other details
        :return: Text string indicating whether or not anything was found and cached or if a record already existed
        when the function ran
        '''
        if sppin_source not in self.sppin_collections:
            raise ValueError("The sppin_source parameter must be one of a list of configured data collections")

        if message_id is None:
            message = self.get_message(message_queue)
            if message is None:
                raise ValueError("There is no available message that can be processed")

            message_id = message["id"]
            message_body = message["body"]

        try:
            sppin_key, sppin_key_type, sppin_key_value, sppin_data = self.check_sppin_key(
                message_body,
                sppin_collections=[sppin_source]
            )
        except Exception as e:
            return e

        if sppin_data[sppin_source] is not None:
            try:
                self.delete_message(message_queue, message_id)
            except:
                pass
            return f"ALREADY CACHED: {sppin_key}"

        if message_body["source"]["type"] == "ScienceBase Source File":
            name_source = message_body["source"]["sciencebase_source_file"]
            source_date = message_body["source"]["sciencebase_source_file_date"]
        else:
            name_source = message_body["source"]["name_source"]
            source_date = datetime.utcnow().isoformat()

        taxa_summary_msg = None
        name_queue = None
        worms_queue = None

        # Run the different types of pysppin processors
        if sppin_source == "itis":
            source_results = pysppin.itis.ItisApi().search(
                sppin_key,
                name_source=name_source,
                source_date=source_date
            )

            taxa_summary_msg, name_queue, worms_queue = self.process_itis_result(source_results)

        elif sppin_source == "worms":
            source_results = pysppin.worms.Worms().search(
                sppin_key,
                name_source="SGCN",
                source_date=source_date
            )

            taxa_summary_msg, name_queue = self.process_worms_result(source_results)

        elif sppin_source == "gbif":
            source_results = pysppin.gbif.Gbif().summarize_us_species(
                sppin_key,
                name_source=name_source
            )

        elif sppin_source == "ecos":
            source_results = pysppin.ecos.Tess().search(sppin_key)

        elif sppin_source == "iucn":
            source_results = pysppin.iucn.Iucn().search_species(
                sppin_key,
                name_source=name_source
            )

        elif sppin_source == "natureserve":
            source_results = pysppin.natureserve.Natureserve().search(
                sppin_key,
                name_source=name_source
            )


        # Pass on messages to additional queues
        if taxa_summary_msg is not None:
            self.queue_message(
                queue_name="mq_taxa_summary",
                message=taxa_summary_msg
            )

        if name_queue is not None:
            self.queue_message(
                queue_name=[
                    "mq_ecos_check",
                    "mq_iucn_check",
                    "mq_natureserve_check",
                    "mq_gbif_check"
                ],
                message=name_queue
            )

        if worms_queue is not None:
            self.queue_message(
                queue_name="mq_worms_check",
                message=worms_queue
            )

        # Insert results into appropriate sppin container
        self.cache_sppin(
            sppin_source=sppin_source,
            sppin_data=source_results
        )

        # Delete processed message
        try:
            self.delete_message(message_queue, message_id)
        except:
            pass

        return f"MESSAGE PROCESSED: {sppin_key}"

    def process_sgcn_source_record(self, record):
        '''
        This function processes an individual source record from any SGCN source, validates it against a schema,
        and pushes a valid record into a database.

        :param record:
        :return: nothing
        '''
        schema = self.get_schema("sgcn_source_records_schema")

        for record in pysppin_utils.validate_data(record, schema):
            if record["valid"]:
                self.sql_data.insert_record("sgcn", "sgcn", record["record"], mq=False)
            else:
                self.queue_message(queue_name="mq_invalid_source", message=record["record"])

    def queue_message(self, queue_name, message):
        if isinstance(queue_name, str):
            if isinstance(message, dict):
                self.sql_mq.insert_record("mq", queue_name, message, mq=True)
            elif isinstance(message, list):
                for msg in message:
                    self.sql_mq.insert_record("mq", queue_name, msg, mq=True)

        elif isinstance(queue_name, list):
            for q in queue_name:
                if isinstance(message, dict):
                    self.sql_mq.insert_record("mq", q, message, mq=True)
                elif isinstance(message, list):
                    for msg in message:
                        self.sql_mq.insert_record("mq", q, msg, mq=True)

    def get_message(self, queue_name):
        return self.sql_mq.get_single_record("mq", queue_name)

    def delete_message(self, queue_name, identifier):
        return self.sql_mq.delete_record("mq", queue_name, identifier)

    def get_records_by_sppin_key(self, sppin_key, ids_only=False):
        records = self.sql_data.get_select_records(
            "sgcn",
            "sgcn",
            "sppin_key = ?",
            sppin_key
        )

        if ids_only and records is not None:
            return [i["id"] for i in records]

        return records

    def update_taxa_summary_data(self, sppin_key, summary):
        '''
        This function infuses taxonomic authority summary properties into master SGCN records based on the sppin_key
        identifier.

        :param sppin_key: Compound key containing the type of value and value, either scientific name or ITIS TSN
        :param summary: Dictionary containing key value pairs of summary information
        :return: Summary list of updates committed
        '''
        taxonomic_authority = summary["taxonomic_authority_url"].split("/")[2].split(".")[1].lower()

        sgcn_records = self.get_records_by_sppin_key(sppin_key)

        if sgcn_records is None or len(sgcn_records) == 0:
            return None

        if taxonomic_authority == "marinespecies":
            ids_to_update = [i["id"] for i in sgcn_records if i["taxonomic_authority_url"] is None]
        else:
            ids_to_update = [i["id"] for i in sgcn_records]

        return self.sql_data.insert_sppin_props(
            db_name="sgcn",
            table_name="sgcn",
            props=summary,
            identifiers=ids_to_update
        )

    def cache_sppin(self, sppin_source, sppin_data, cache_type="sqlite"):
        '''
        Caches sppin data into a data store. The cache_type parameter specifies where to send the data.

        :param sppin_source: Logical name of the sppin source
        :param sppin_data: Dictionary containing sppin data to be cached
        :param cache_type: Set to a particular type to control where the data is sent. Defaults to local processing
        into a sqlite database
        :return: Dependent on the cache_type. In the case of sqlite, returns the unique identifier of the inserted
        record
        '''
        if cache_type == "sqlite":
            return self.sql_sppin.insert_record(
                "sppin",
                sppin_source,
                sppin_data
            )

        else:
            return None
