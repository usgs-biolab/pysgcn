# pysgcn

This package provides the necessary steps to assemble the Synthesized Species of Greatest Conservation Need database from source files in ScienceBase.

## Workflow

The IPython Notebooks in the workflow folder provide a recipe for processing the SGCN source data using a combination of the pysgcn functions and pysppin modules.

1) Get Processable SGCN Items - The get_processable_items function from pysgcn provides the basic logic for querying ScienceBase and returning processable item metadata (simplified set of properties needed to process source data). The workflow step in the standalone package sends processable items to a data table as messages, and can be modified to send messages to an actual message queue.
2) Extract Source Data - This workflow step includes functions to pull source data from ScienceBase files, add records to a queue for processing, and add taxa names to a queue for lookup against ITIS (next step with scientific names before additional information gathering steps).
3) Process Source Data Messages - This workflow step can start executing as soon as there are source data messages in the queue. It processes the source records, validates them against a schema, and puts them into a database table.


## Provisional Software Statement

Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.
