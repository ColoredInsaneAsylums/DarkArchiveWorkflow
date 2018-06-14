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
# Update: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#

import metadatautilspkg.globalvars as globalvars

# ERROR CODES
ERROR_INVALID_ARGUMENT_STRING = {"code": "e01", "message": "Invalid (number of) arguments specified on the command-line."}
ERROR_CANNOT_OPEN_CSV_FILE = {"code": "e02", "message": "Cannot open the CSV batch file '{}'.".format(globalvars.csvFile)}
ERROR_CANNOT_WRITE_CSV_FILE = {"code": "e03", "message": "Could not write CSV file for errors encountered during transfers."}
ERROR_CANNOT_READ_DBCONF_FILE = {"code": "e04", "message": "Cannot read the DB configuration file."}
ERROR_INVALID_HEADER_ROW = {"code": "e05", "message": "The header in the input CSV file is invalid."}
ERROR_CANNOT_CONNECT_TO_DB = {"code": "e06", "message": "Cannot connect to the DB."}
ERROR_CANNOT_AUTHENTICATE_DB_USER = {"code": "e07", "message": "Cannot authenticate DB user specified."}
ERROR_CANNOT_INSERT_INTO_DB = {"code": "e08", "message": "Cannot insert record into the DB."}
ERROR_CANNOT_REMOVE_FILE = {"code": "e09", "message": "Cannot remove file from directory."}
ERROR_CANNOT_REMOVE_RECORD_FROM_DB = {"code": "e10", "message": "Cannot remove record from DB."}
ERROR_CANNOT_READ_LABELS_FILE = {"code": "e11", "message": "Cannot read the labels file '{}'.".format(globalvars.labelsFileName)}
ERROR_INVALID_JSON_IN_LABELS_FILE = {"code": "e12", "message": "The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.labelsFileName)}
ERROR_CANNOT_READ_VOCAB_FILE = {"code": "e13", "message": "Cannot read the vocab file '{}'.".format(globalvars.vocabFileName)}
ERROR_INVALID_JSON_IN_VOCAB_FILE = {"code": "e14", "message": "The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.vocabFileName)}
ERROR_CANNOT_CREATE_DESTINATION_DIRECTORY = {"code": "e15", "message": "Cannot create destination directory '{}'. Skipping to next transfer."}
ERROR_CANNOT_FIND_DIRECTORY = {"code": "e16", "message": "Cannot find the filepath directory. '{}'."}
ERROR_CANNOT_FIND_DOCUMENT = {"code": "e17", "message": "Document not found in the database. '{}'."}
ERROR_CANNOT_FILE_EXTENSION = {"code": "e18", "message": "File Extension does not match. '{}'."}
ERROR_INSTALL_IMAGEMAGICK = {"code": "e19", "message": "Image Magick is not installed."}
ERROR_CANNOT_UPDATE_DB = {"code": "e20", "message": "Updation of the record failed."}
ERROR_EXT_ARGUMENT = {"code": "e21", "message": "Required argument '-e' not passed in the command line."}
ERROR_FILE_ARGUMENT = {"code": "e22", "message": "Required argument '-f' not passed in the command line."}
ERROR_TECH_UPDATED = {"code": "e23", "message": "The technical property profile for the file '{}' has been already updated."}
ERROR_COM_UPDATED = {"code": "e24", "message": "The compliance metadata profile has been already updated."}
