

## mirrulations-document-sqlite

This repo contains scripts to put the document JSON files in a sqlite database and then analyze the data.


## Obtaining Data


All data comes from the [mirrulations S3 bucket](https://registry.opendata.aws/mirrulations/), which is a part of the
[AWS Registry of Open Data](https://registry.opendata.aws/).  The fastest way to obtain the data is to use the
[AWS CLI](https://aws.amazon.com/cli/).  The following assumes you have the AWS CLI installed an configured.

The following command will download all Docket and Document JSON files in the bucket.  This command
will download approximately 9.1 GB of data in about 2.2 million files.  It will take 40 minutes or more to run.

```
aws s3 sync s3://mirrulations/raw-data/ data --exclude "*" --include "*/text-*/docket/*.json" --include "*/text-*/documents/*.json" --only-show-errors
```


## Create the Database

The file `documents_schema.sql` contains schema for a table that contains *most* of the data in each document JSON file.
This schema was created based on the results of `analyze_null_fileds.py`, which produces a report of how often
the various fields are null.

To create the table in a database named `documents.db`, execute:

```
sqlite3 documents.db < documents_schema.sql
```

## Populate the Documents Table

The script `insert_documents.py` will walk the the data in the `data` folder, and insert the data for each document JSON file
into the Documents table.


```
python3 insert_documents.py
```


## 42 CFR Part Analysis

"Public Health" is [Title 42 of the Code of Federal Regulations](https://www.ecfr.gov/current/title-42).  The script `count_42_cfr_parts.py`
queries the `documents.db` for all **documents** where the `document_type` is "Rule" and the `cfr_part` contains a number related to title 42.  The query
provides the docket ID and the CFR Part string for all documents that match, and the script processes the CFR Part string to extract all part numbers.
The result is a table with Part Number, Count, and a list of Agencies (with counts) that mention this part number.

NOTE:  The `cfr_part` field is messy.  This script was a quick-and-dirty analysis using regular expressions.  It simply matches numbers, and it won't 
handle ranges (e.g. 410-415) or other text that infers parts.

A sample run of this script produced:

```
Part            Count  Agencies
------------------------------------------------------------
412               164  CMS(153) HHS(11)
413               123  CMS(120) HHS(3)
414               121  CMS(117) HHS(4)
410               107  CMS(97) HHS(10)
405                97  CMS(93) HHS(4)
424                96  CMS(88) HHS(8)
423                79  CMS(70) HHS(8) MSHA(1)
411                76  CMS(75) HHS(1)
422                68  CMS(62) HHS(6)
419                61  CMS(51) HHS(10)
495                59  CMS(52) HHS(7)
416                58  CMS(48) HHS(10)
447                47  CMS(45) HHS(2)
438                45  CMS(35) HHS(10)
457                45  CMS(37) HHS(8)
431                44  CMS(38) HHS(6)
488                43  CMS(41) HHS(2)
489                43  CMS(43)
```

## 42 CFR 412 Dockets

The Script `list_42_cfr_412_dockets.py` finds all dockets that contain a document that have `document_type` of "Rule" and at least one document
with `cfr_part` referring to title 42 part 412.  It puts these in order based on the `modified_date`, and it outputs the docket ID, the 
modified date, and the title.
