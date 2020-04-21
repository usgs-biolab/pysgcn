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

    # Stage 2 Cache Metadata and Document Schemas
    sgcn_meta = sgcn.cache_sgcn_metadata(return_data=True)
    # Stage 1 Get Processable SGCN Items
    process_items = sgcn.get_processable_items()

    record_count = 0
    # for debugging
    debug_process_count = 0
    debug_process_start = 0
    debug_total_to_process = 6

    for item in process_items:
        # species extracted from the source file to process
        species = []
        if debug_process_count < debug_process_start or debug_process_count > debug_total_to_process:
            debug_process_count += 1
            continue
        print("processing {} {}".format(item["state"], item["year"]))

        # Stage 3 Extract Source Data
        res = sgcn.process_sgcn_source_item(item, metadata_cache=sgcn_meta)

        # Stage 4 Process Source Data
        for spec in res:
            try:
                # validate data against the json schema
                valid = sgcn.validate_data(spec)
                # create a hash of the species record so we don't add duplicates from the same file
                hsh = hashlib.sha1(repr(json.dumps(spec, sort_keys=True)).encode('utf-8')).hexdigest()
                # make sure we don't add duplicates by comparing hashs
                if valid and hsh not in map(lambda species_tup: species_tup[1], species):
                    species.append((hsh, spec))
                else:
                    print('Duplicate species found: ', spec["scientific name"])
            except Exception as e:
                print('Error (process_1): Species: "{}"'.format(spec["scientific name"]))
                print("Error (process_1): ", e)
        for hsh, spec in species:
            # use the hash as an id for the rest of the processing
            species_result = {"id": hsh, **spec}
            # send onto the next stage
            send_to_stage(species_result, 2)
            record_count += 1

        debug_process_count += 1
    # return the total number of species found in all processed files
    return record_count

def process_2(
    path,
    ch_ledger,
    send_final_result,
    send_to_stage,
    previous_stage_result,
    cache_manager,
):
    sgcn = pysgcn.Sgcn(operation_mode='pipeline', cache_manager=cache_manager)

    print('--- start species', previous_stage_result["sppin_key"], ' ---')
    # Stage 5 ITIS, WoRMS
    taxa_summary_msg, name_queue = sgcn.gather_taxa_summary(previous_stage_result)

    # create the final record
    sgcn_record = {"row_id": previous_stage_result["id"], "data": previous_stage_result}
    if taxa_summary_msg:
        # Infuse Taxonomic Summary
        sgcn_record["data"] = {**sgcn_record["data"], **taxa_summary_msg}

    # send the final result to the database
    send_final_result(sgcn_record)

    # check to see if additional data needs to be gathered
    if name_queue:
        # send to the next data gathering stage
        send_to_stage({"name_queue": name_queue, "sppin_source": "gbif"}, 3)
        send_to_stage({"name_queue": name_queue, "sppin_source": "ecos"}, 3)
        send_to_stage({"name_queue": name_queue, "sppin_source": "iucn"}, 3)
        send_to_stage({"name_queue": name_queue, "sppin_source": "natureserve"}, 3)

    print('--- end species', previous_stage_result["sppin_key"], ' ---')

def process_3(
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
