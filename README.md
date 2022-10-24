# Description

This tool is used to identify gis projects in a specific type of dataset.

Invoke the tool by running python gis_processor.py with the following commands:
* `g-json` Generate the gis_info.json file.
* `move` Move files according to the gis_info.json file.
Running the script with no commands defaults to `g-json` followed by `move`

The gis_info.json file is placed in the same folder as the av_db file.
A gis log file is placed in the _metadata folder of the root data directory 
(usually the OriginalFiles or OriginalDocuments folder).

# Dependencies
This tool only uses the standard library shipped with Python 3.
The only dependecy that needs to be installed in order to run the tool
is an up to date version of Python 3.
