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
    print(len(process_items))

    record_count = 0
    # records_to_send = 20
    process_count = 0
    process_start = 0
    total_to_process = 6

    for item in process_items:
        species = []
        if process_count < process_start or process_count > total_to_process:
            process_count += 1
            continue
        print("processing {} {}".format(item["state"], item["year"]))

        # Stage 3 Extract Source Data
        res = sgcn.process_sgcn_source_item(item, metadata_cache=sgcn_meta)

        # Stage 4 Process Source Data
        for spec in res:
            try:
                valid = sgcn.validate_data(spec)
                hsh = hashlib.sha1(repr(json.dumps(spec, sort_keys=True)).encode('utf-8')).hexdigest()
                # make sure we don't add duplicates
                if valid and hsh not in map(lambda species_tup: species_tup[1], species):
                    species.append((hsh, spec))
            except Exception as e:
                print(spec["scientific name"])
                print(e)
        for hsh, spec in species:
            # if record_count < records_to_send:
            species_result = {"id": hsh, **spec}
            send_to_stage(species_result, 2)
            record_count += 1

        process_count += 1
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

    # Stage 5 ITIS, WoRMS
    print('--- start species', previous_stage_result["sppin_key"], ' ---')
    taxa_summary_msg, name_queue = sgcn.gather_taxa_summary(previous_stage_result)

    sgcn_record = {"row_id": previous_stage_result["id"], "data": previous_stage_result}
    if taxa_summary_msg:
        # Infuse Taxonomic Summary
        sgcn_record["data"] = {**sgcn_record["data"], **taxa_summary_msg}

    send_final_result(sgcn_record)

    if name_queue:
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
