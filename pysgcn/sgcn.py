from sciencebasepy import SbSession
import pandas as pd
from pandas.io.json import json_normalize
import requests
from datetime import datetime
from pysppin import pysppin
import os

common_utils = pysppin.utils.Utils()
itis_api = pysppin.itis.ItisApi()
worms = pysppin.worms.Worms()


class Sgcn:
    def __init__(self):
        self.description = "Set of functions for assembling the SGCN database"
        self.sgcn_root_item = '56d720ece4b015c306f442d5'

        self.sb = SbSession()
        self.sgcn_base_item = self.sb.get_item(self.sgcn_root_item)

        self.historic_national_list_file = next(
            (f["url"] for f in self.sgcn_base_item["files"] if f["title"] == "Historic 2005 SWAP National List"), None)
        self.sgcn_itis_overrides_file = next((f["url"] for f in self.sgcn_base_item["files"]
                                              if f["title"] == "SGCN ITIS Overrides"), None)

    def get_sciencebase_source_items(self):
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

        return source_items

    def historic_list(self):
        if self.historic_national_list_file is None:
            return None
        else:
            return requests.get(self.historic_national_list_file).text.split("\n")

    def check_historic_list(self, scientificname):
        historic_spp_list = self.historic_list()

        if historic_spp_list is None:
            return False
        if scientificname in historic_spp_list:
            return True
        else:
            return False

    def sgcn_itis_overrides(self):
        if self.sgcn_itis_overrides_file is None:
            return None
        else:
            return pd.read_json(self.sgcn_itis_overrides_file).to_dict("records")

    def check_itis_override(self, scientific_name):
        itis_overrides = self.sgcn_itis_overrides()

        if itis_overrides is None:
            return None

        return next(
            (i["taxonomicAuthorityID"] for i in itis_overrides if i["ScientificName_original"] == scientific_name),
            None)

    def get_processable_items(self, sb_item_list=None):
        if sb_item_list is None:
            sb_item_list = self.get_sciencebase_source_items()

        processable_sgcn_items = [
            {
                "sciencebase_item_id": i["link"]["url"],
                "state": next(t["name"] for t in i["tags"] if t["type"] == "Place"),
                "year": next(d["dateString"] for d in i["dates"] if d["type"] == "Collected"),
                "source_file_url": next(f["url"] for f in i["files"] if f["title"] == "Process File"),
                "source_file_date": next(f["dateUploaded"] for f in i["files"] if f["title"] == "Process File")
            }
            for i in sb_item_list
            if next((f for f in i["files"] if f["title"] == "Process File"), None) is not None
        ]

        unprocessed_items = [i for i in processable_sgcn_items if self.check_existing_process_log(i) is None]

        return unprocessed_items

    def check_existing_process_log(self, item):
        process_log = list()

        return next((i for i in process_log if len(process_log) > 0 and
                     i["source_file_url"] == item["source_file_url"] and
                     i["source_file_date"] == item["source_file_date"]),
                    None)

    def process_sgcn_source_item(self, item, output_type="dict"):
        try:
            df_src = pd.read_csv(item["source_file_url"], delimiter="\t")
        except UnicodeDecodeError:
            df_src = pd.read_csv(item["source_file_url"], delimiter="\t", encoding='latin1')

        # Make lower case columns to deal with slight variation in source files
        df_src.columns = map(str.lower, df_src.columns)

        # Include the source item identifier
        df_src["sciencebase_item_id"] = item["sciencebase_item_id"]

        # Include a processing date
        df_src["record_processed"] = datetime.utcnow().isoformat()

        # Set the file date and url from the ScienceBase file to each record in the dataset for future reference
        df_src["source_file_date"] = item["source_file_date"]
        df_src["source_fileurl"] = item["source_file_url"]

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
        df_src["itis_identifier"] = df_src.apply(lambda x: self.check_itis_override(x["scientific name"]), axis=1)

        if output_type == "dataframe":
            return df_src
        elif output_type == "dict":
            return df_src.to_dict("records")
        elif output_type == "json":
            return df_src.to_json(orient="records")

    def cache_item_data(self, item, cache_location=os.getenv("DATA_CACHE"), dataset=None):
        cache_path = f"{cache_location}/sgcn"

        try:
            os.makedirs(cache_path)
        except FileExistsError:
            pass

        db_file = f"{item['sciencebase_item_id'].split('/')[-1]}_{item['source_file_date']}"

        if os.path.exists(f"{cache_path}/{db_file}"):
            dataset = pd.read_feather(f"{cache_path}/{db_file}")
        else:
            if dataset is None:
                dataset = self.process_sgcn_source_item(item, output_type="dataframe")

            dataset.to_feather(f"{cache_path}/{db_file}")

        return dataset

    def sgcn_source(self, cache_path=os.getenv('DATA_CACHE')):
        sgcn_cache = f"{cache_path}/sgcn"

        for (dirpath, dirnames, filenames) in os.walk(sgcn_cache):
            file_paths = [f"{dirpath}/{f}" for f in filenames]
            break

        all_datasets = list()
        for file in file_paths:
            all_datasets.append(pd.read_feather(file))

        df_all_data = pd.concat(all_datasets, ignore_index=True, sort=False)
        unique_tsn_list = [i.split(":")[-1] for i in
                           df_all_data.loc[df_all_data["itis_identifier"].notnull()]["itis_identifier"].
                               unique().tolist()]
        unique_name_list = df_all_data.loc[df_all_data["clean_scientific_name"].notnull()]["clean_scientific_name"].\
            unique().tolist()

        return df_all_data, unique_name_list, unique_tsn_list


