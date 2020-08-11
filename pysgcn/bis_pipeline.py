import json
import hashlib
from . import sgcn as pysgcn
import pysppin

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

    for item in process_items:
        send_to_stage(item, 2)

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
        if mapping['rank'] == "Class":
            taxodata = {'taxoname' : mapping['name'], 'taxogroup' : mapping['sgcntaxonomicgroup']}
            class_list.append(taxodata)

    # species extracted from the source file to process
    species = []

    print("processing {} {}".format(previous_stage_result["state"], previous_stage_result["year"]))

    # Stage 3 Extract Source Data
    res = sgcn.process_sgcn_source_item(previous_stage_result, metadata_cache=sgcn_meta)

    # Stage 4 Process Source Data
    for spec in res:
        if spec['scientific name'] != "Typhlatya monae" and spec['scientific name'] != "Megaptera novaeangliae" and spec['scientific name'] != "Orbicella annularis" and spec['scientific name'] != "Plectomerus sloatianus":
            continue
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
    print('--- species {} ({})  '.format(previous_stage_result["scientific name"], previous_stage_result['common name']), end='')

    # Stage 5 ITIS, WoRMS
    taxa_summary_msg, name_queue = sgcn.gather_taxa_summary(previous_stage_result)

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
    if "commonname" in sgcn_record["data"].keys():
        print('    commonname: {}'.format(sgcn_record["data"]["commonname"]))
    if "taxonomic_authority_url" in sgcn_record["data"].keys():
        print('    taxonomic_authority_url: {}'.format(sgcn_record["data"]["taxonomic_authority_url"]))
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
