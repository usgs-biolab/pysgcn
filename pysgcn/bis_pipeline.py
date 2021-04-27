import json
import hashlib
from . import sgcn as pysgcn
import pysppin
from pysgcn import validate_sgcn_input
import requests

json_schema = None

# This architecture and process is based on the pipeline documentation here: https://code.chs.usgs.gov/fort/bcb/pipeline/docs

def process_1(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
    cache_manager,
):
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager=cache_manager)

    # Stage 1 Get Processable SGCN Items
    process_items = sgcn.get_processable_items()

    test_data = list()
    # This is to allow the test data set to be reduced to targeted State/year
    # combos for local debugging.  We need at least 2 pairs in this list for processing
    # to work properly due to python's interpretation of a list...
    #test_data = (("placeholder", "1000"), ("Puerto Rico", "2015"), ("xNorth Dakota", "2015"), ("xOhio", "2015"), ("xOklahoma", "2015"), ("xOregon", "2015"))

    processed_items = set()
    for item in process_items:
        if not test_data or in_test_data(item, test_data):
            # BCB-1586 Prevent duplicate processing that sometimes happens in the AWS space.
            if item['sciencebase_item_id'] not in processed_items:
                processed_items.add(item['sciencebase_item_id'])
                send_to_stage(item, 2)

    # We cannot validate a pipeline run this way.  Locally it works because everything
    # is synchronous and by the time it gets to this chunk of code, the pipeline has completed.
    # However, in the AWS lambda cloud environment, everything is asynchronous and when we
    # reach this chunk of code, the pipeline is FAR from being complete.
    # This validation will have to be done manually outside of the pipeline.
    # I will leave this code here so we can see what needs to be done to validate the pipeline data.
    if not test_data and False:
        rawdata = validate_sgcn_input.validate_latest_run(False)
        pipeline_id = rawdata['pipeline_id']
        data = dict()
        data['totals'] = rawdata['totals']
        data['states'] = rawdata['states']

        print('Adding validation results for: {} : {}'.format(pipeline_id, json.dumps(data)))
        cache_manager.add_to_cache(pipeline_id, json.dumps(data))

def in_test_data(item, test_data):
    state = item['state']
    year = item['year']
    for pair in test_data:
        if pair[0] == state and pair[1] == year:
            return True
    return False

def process_2(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
    cache_manager,
):
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager=cache_manager)

    record_count = 0
    # Stage 2 Cache Metadata and Document Schemas
    sgcn_meta = sgcn.cache_sgcn_metadata(return_data=True)

    # BCB-1556
    class_list = list()
    for mapping in sgcn_meta["Taxonomic Group Mappings"]:
        if mapping['rank'].lower() == "class":
            taxodata = {'taxoname' : mapping['name'], 'taxogroup' : mapping['sgcntaxonomicgroup']}
            class_list.append(taxodata)

    # species extracted from the source file to process
    species = []

    print("processing {} {}".format(previous_stage_result["state"], previous_stage_result["year"]))

    # Stage 3 Extract Source Data
    res = sgcn.process_sgcn_source_item(previous_stage_result, metadata_cache=sgcn_meta)

    # Test species set that tests ITIS and WoRMS searches without having to run entire
    # data set even on the pared down TEST data site we use.
    # Uncomment one of the following lines
    testSpecies = None
    #testSpecies = ["Typhlatya monae", "Megaptera novaeangliae", "Orbicella annularis", "Plectomerus sloatianus"]

    # Stage 4 Process Source Data
    for spec in res:
        if testSpecies is not None and spec['scientific name'] not in testSpecies:
            continue

        #if spec['scientific name'] != "Typhlatya monae" and spec['scientific name'] != "Megaptera novaeangliae" and spec['scientific name'] != "Orbicella annularis" and spec['scientific name'] != "Plectomerus sloatianus":
        #    continue
        try:
            # validate data against the json schema
            valid = sgcn.validate_data(spec)
            # create a hash of the species record so we don't add duplicates from the same file
            hsh = hashlib.sha1(repr(json.dumps(spec, sort_keys=True)).encode('utf-8')).hexdigest()
            # make sure we don't add duplicates by comparing hashs
            if valid and hsh not in map(lambda species_tup: species_tup[0], species):
                species.append((hsh, spec))
                # use the hash as an id for the rest of the processing
                species_result = {"id": hsh, **spec}
                # BCB-1556
                species_result["taxogroupings"] = class_list
                # send onto the next stage
                send_to_stage(species_result, 3)
                record_count += 1
            else:
                print('Invalid or Duplicate species found: ', spec["scientific name"])
        except Exception as e:
            print('Error (process_1): Species: "{}"'.format(spec["scientific name"]))
            print("Error (process_1): ", e)

    # return the number of species for this process file
    return record_count

def process_3(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
    cache_manager,
):
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager=cache_manager)

    # Stage 5 ITIS, WoRMS
    taxa_summary_msg, name_queue = sgcn.gather_taxa_summary(previous_stage_result)
    if taxa_summary_msg and 'commonname' in taxa_summary_msg.keys(): 
        common_name = taxa_summary_msg['commonname']
    else:
        common_name = previous_stage_result['common name']
    print('--- species {} ({})  '.format(previous_stage_result["scientific name"],common_name), end='')

    # BCB-1556
    class_name = "none"
    if taxa_summary_msg:
        if "class_name" in taxa_summary_msg.keys():
            class_name = taxa_summary_msg['class_name']

    taxo_group = next((tg["taxogroup"] for tg in previous_stage_result["taxogroupings"] if tg["taxoname"] == class_name), None)

    # Erase all the taxogroupings data so it doesn't go into the DB
    previous_stage_result["taxogroupings"] = None

    # create the final record
    sgcn_record = {"row_id": previous_stage_result["id"], "data": previous_stage_result}
    if taxa_summary_msg:
        # Infuse Taxonomic Summary
        sgcn_record["data"] = {**sgcn_record["data"], **taxa_summary_msg}
        # BCB-1556
        if taxo_group:
            sgcn_record["data"]["taxonomic category"] = taxo_group

    print('     class({})  taxogroup({})  sgcnTaxoGroup({})'.format(class_name, taxo_group, sgcn_record["data"]["taxonomic category"]))

    sgcn_record['data']['nationallist'] = False
    if "taxonomic_authority_url" in sgcn_record['data'].keys():
        if sgcn_record['data']['taxonomic_authority_url'].startswith("http"):
            sgcn_record['data']['nationallist'] = True

    if "historic_list" in sgcn_record['data'].keys():
        if sgcn_record["data"]["historic_list"] == True:
            sgcn_record['data']['nationallist'] = True

    validateSGCNRecord(sgcn_record)

    ecos_entry = get_single_ecos_entry(previous_stage_result["scientific name"])
    sgcn_record["data"]["ecos"] = dict()
    if ecos_entry:
        sgcn_record["data"]["ecos"]["url"] = ecos_entry["url"]
        sgcn_record["data"]["ecos"]["ESA_listings"] = ecos_entry["ESA_listings"]
    else:
        sgcn_record["data"]["ecos"] = "no ecos data"

    # send the final result to the database
    send_final_result(sgcn_record)

    # commented out for now because we're not using this data yet and it's the most time consuming processing
    # # check to see if additional data needs to be gathered
    # if name_queue:
    #     # send to the next data gathering stage
    #     send_to_stage({"name_queue": name_queue, "sppin_source": "gbif"}, 3)
    #     send_to_stage({"name_queue": name_queue, "sppin_source": "ecos"}, 3)
    #     send_to_stage({"name_queue": name_queue, "sppin_source": "iucn"}, 3)
    #     send_to_stage({"name_queue": name_queue, "sppin_source": "natureserve"}, 3)

def validateSGCNRecord(record):
    badFields = list()
    data = record['data']
    keys = data.keys()
    check(data, keys, "scientific name", badFields)
    check(data, keys, "common name", badFields)
    check(data, keys, "taxonomic category", badFields)
    check(data, keys, "state", badFields)
    check(data, keys, "sciencebase_item_id", badFields)
    check(data, keys, "record_processed", badFields)
    check(data, keys, "source_file_date", badFields)
    check(data, keys, "source_file_url", badFields)
    check(data, keys, "year", badFields)
    check(data, keys, "clean_scientific_name", badFields)
    check(data, keys, "historic_list", badFields)
    check(data, keys, "scientificname", badFields)
    check(data, keys, "taxonomicrank", badFields)
    check(data, keys, "taxonomic_authority_url", badFields)
    check(data, keys, "match_method", badFields)
    check(data, keys, "commonname", badFields)
    check(data, keys, "class_name", badFields)
    check(data, keys, "nationallist", badFields)

    if badFields:
        print('    Warning: SGCN Record: {}'.format(data['id']))
        print('       missing fields: {}'.format(badFields))
    if data["nationallist"] == False:
        print('    Warning: SGCN Record: {} NOT on NationalList'.format(data['id']))

def check(data, keys, name, badFields):
    if name not in keys or data[name] == "":
        badFields.append(name)

def process_4(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
    cache_manager,
):
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager=cache_manager)
    # ECOS TESS, IUCN, NatureServe, GBIF
    sgcn.gather_additional_cache_resources(previous_stage_result["name_queue"], previous_stage_result["sppin_source"])

def get_single_ecos_entry(entry):
    # Search for a single entry by scientific name in ecos
    response = requests.get("https://ecos.fws.gov/ecp/pullreports/catalog/species/report/species/export?columns=/species@cn,sn,status,desc,listing_date&filter=/species@sn+=+%27" + entry + "%27&format=json&limit=100&sort=/species@cn+asc;/species@sn+asc")
    single_response = response.json()
    data = single_response['data']
    if len(data) == 0: return None
    cleaned_response = dict()
    cleaned_response['commonname'] = data[0][0]
    cleaned_response['scientificname'] = data[0][1]['value']
    cleaned_response['url'] = data[0][1]['url']
    cleaned_response['ESA_listings'] = list()
    for entry in data:
        record = {'status' : entry[2], 'location' : entry[3], 'date' : entry[4]}
        cleaned_response['ESA_listings'].append(record)
    return cleaned_response
