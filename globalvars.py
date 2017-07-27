# IMPORT NEEDED MODULES
import csv
import sys
import getopt
import os
import glob
import shutil
import hashlib
import pymongo
from pymongo import MongoClient
#from bson.objectid import ObjectId
from uuid import uuid4
from datetime import datetime
from time import localtime, time, strftime
import json
import urllib
import argparse
from collections import namedtuple

# DECLARE GLOBALS AND THEIR DEFAULT VALUES
ext = "*"  # Extension, with a default value of *
move = False  # If move is True, the copying will be destructive
batchMode = False  # If copying/moving will be done in a batch (with a -f 
                   # option). Disabled by default.
csvFile = ""  # Path to the CSV file
quietMode = False  # Quiet output mode, disabled by default
transferList = []  # List of source-dest pairs to be processed. Each pair would
                   # itself be a two-element list, with the SOURCE at index 0,
                   # and DESTINATION at index 1.
errorList = []  # List of source-dest pairs for which
                                         # errors were encountered during
                                         # processing. Is a subset of 
                                         # transferList.

minNumCols = 2  # The minimum no. of columns that should be present in each row
                # of the CSV file. Determined by the header row.


# DATABASE VARIABLES
DBNAME = "cshdb" # TODO: Move this to a config file, along with other db stuff
dbHandle = None # Stores the handle to access the database. Initialized to None.
dbCollection = None

# LABEL DICTIONARIES
LABELS_FILE = "labels.json"
labels = {}

# CONTROLLED VOCABULARY
VOCAB_FILE = "vocab.json"
vocab = {}

# ERROR CODES
ERROR_INVALID_ARGUMENT_STRING = -1
ERROR_CANNOT_OPEN_CSV_FILE = -2
ERROR_CANNOT_WRITE_CSV_FILE = -3
ERROR_CANNOT_READ_DBCONF_FILE = -4
ERROR_INVALID_HEADER_ROW = -5
ERROR_CANNOT_CONNECT_TO_DB = -6
ERROR_CANNOT_AUTHENTICATE_DB_USER = -7
ERROR_CANNOT_INSERT_INTO_DB = -8
ERROR_CANNOT_REMOVE_FILE = -9
ERROR_CANNOT_REMOVE_RECORD_FROM_DB = -10
ERROR_CANNOT_READ_LABELS_FILE = -11
ERROR_KEY_NOT_FOUND = -12
ERROR_INVALID_JSON_FILE = -13
ERROR_CANNOT_CREATE_DESTINATION_DIRECTORY = -14
ERROR_CANNOT_READ_VOCAB_FILE = -15

# CSV FILE RELATED CONSTANTS
CSV_COL_1_NAME = "source"
CSV_COL_2_NAME = "destination"

# METADATA-RELATED CONSTANTS
OBJ_ID_TYPE = "UUID"
EVT_ID_TYP = "UUID"
LNK_AGNT_ID_TYPE = "program"
PYTHON_VER_STR = "Python " + sys.version.split(' ')[0]
LNK_AGNT_ID_VAL = PYTHON_VER_STR + "; " + sys.argv[0]
MD_INIT_STRING = ""
CHECKSUM_ALGO = "MD5"
CHECKSUM_METHOD = "hashlib.md5()"
EVT_OUTCM_SUCCESS = "00"
EVT_OUTCM_FAILURE = "FF"

EVT_DTL_REPLICATION = PYTHON_VER_STR + "; shutil.copy"
EVT_DTL_FILENAME_CHNG = PYTHON_VER_STR + "; os.rename"

UNIQUE_ID_ALGO = "UUID v4"
UNIQUE_ID_METHOD = "uuid.uuid4()"

ARRANGEMENT_INFO_MARKER = "arrange:"
ARRANGEMENT_INFO_LABEL = "arrangementInfo"
ARRANGEMENT_INFO_LABEL_SUFFIX = "Label"