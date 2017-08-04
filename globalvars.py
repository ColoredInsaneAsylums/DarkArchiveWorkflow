import sys

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
dbHandle = None # Stores the handle to access the database. Initialized to None.
dbCollection = None

# LABEL DICTIONARIES
labelsFileName = "labels.json"
labels = {}

# CONTROLLED VOCABULARY
vocabFileName = "vocab.json"
vocab = {}

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


UNIQUE_ID_ALGO = "UUID v4"
UNIQUE_ID_METHOD = "uuid.uuid4()"

ARRANGEMENT_INFO_MARKER = "arrange:"
ARRANGEMENT_INFO_LABEL = "arrangementInfo"
ARRANGEMENT_INFO_LABEL_SUFFIX = "Label"