# -*- coding: utf-8 -*-

# BSD 3-Clause License
# 
# Copyright (c) 2017, ColoredInsaneAsylums
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# 
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# 
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# CREDITS
# Creator: Nitin Verma (nitin dot verma at utexas dot edu)
# 

import sys
import os

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

configDir = "/usr/share/darkarchive"

# LABEL DICTIONARIES
labelsFileName = os.path.join(configDir, "labels.json")
labels = {}

# CONTROLLED VOCABULARY
vocabFileName = os.path.join(configDir, "vocab.json")
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
