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
#
# DETAILS:
# File Name: admin.py
# Description: This file contains source code for the core functionality of the
#              technical schema workflow.
#
# Creator: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#
# IMPORT NEEDED MODULES
import csv
import sys

from datetime import datetime
from time import localtime, time, strftime

from metadatautilspkg.globalvars import *
from metadatautilspkg.errorcodes import *
from metadatautilspkg.dbfunctions import *
from metadatautilspkg.metadatautils import *

def main():

    argParser = defineCommandLineOptions()
    parseCommandLineArgs(argParser, sys.argv[1:])

    print_info("quiet mode: ", globalvars.quietMode)

    if globalvars.batchMode == True:  # Batch mode. Read and validate CSV file.
    # Read CSV file contents into globalvars.technicalList.
        try:
            # Open the CSV file in read-only mode.
            csvFileHandle = open (globalvars.csvFile, "r")
        except IOError as ioErrorCsvRead:
            print_error(ioErrorCsvRead)
            print_error(errorcodes.ERROR_CANNOT_OPEN_CSV_FILE["message"])
            exit(errorcodes.ERROR_CANNOT_OPEN_CSV_FILE["code"])

        # CSV file successfully opened.
        csvReader = csv.reader(csvFileHandle)  # Create an iterable object from the
                                                # CSV file using csv.reader().

        # Extract the first row to check if it is a header.
        firstRow = next(csvReader, None)

        print_info("Checking the header row. Header: {}".format(firstRow))
        if len(firstRow) == 0: # firstRow == None or isFileHeaderValid(firstRow) == False:  # This also serves as a check for an empty CSV file
            print_error(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            globalvars.adminerrorList.append([errorcodes.ERROR_INVALID_HEADER_ROW["message"]])
            errorCSV()
            exit(errorcodes.ERROR_INVALID_HEADER_ROW["code"])

        # Extract Arrange info from header row
        numArrangementInfoCols = 0
        arrangementInfoTags = {}
        for col in firstRow:
            if col.startswith(globalvars.ARRANGEMENT_INFO_MARKER):
                numArrangementInfoCols += 1
                if 'name' in col:
                    arrangementInfoTags[numArrangementInfoCols] = col.split(':')[-1]
                else:
                    arrangementInfoTags[numArrangementInfoCols] = col.split(':')[-1] + globalvars.ARRANGEMENT_INFO_LABEL_SUFFIX

            else:
                print_error("The column names should be with prefix {}".format(globalvars.ARRANGEMENT_INFO_MARKER))

        # globalvars.minNumCols += numArrangementInfoCols
        globalvars.adminerrorList.append(firstRow + ["Comments"])

        # This for loop reads and checks the format (i.errorcodes., presence of at least two
        # columns per row) of the CSV file, and populates 'globalvars.technicalList'

        rowNum = 1
        for row in csvReader:
            if (numArrangementInfoCols % globalvars.minNumCols != 0):  # Check if the row has AT LEAST globalvars.minNumCols elements.
                print_error("Row number {} in {} is not a valid input. This row will not be processed.".format(rowNum, globalvars.csvFile))
                globalvars.adminerrorList.append(row + ["Not a valid input"])
                errorCSV()
            else:
                globalvars.adminList.append(row)
            rowNum += 1

        csvFileHandle.close()  # Close the CSV file as it will not be needed from this point on.

    # print_info("Filepath to extract technical information for: {}".format(len(globalvars.adminList)))

    # READ-IN THE LABEL DICTIONARY
    globalvars.labels = readLabelDictionary()
    print_info("The following labels will be used for labeling metadata items in the database records:")
    print_info(globalvars.labels)

    # READ-IN THE CONTROLLED VOCABULARY
    globalvars.vocab = readControlledVocabulary()

    # CREATE DATABASE CONNECTION
    dbParams = init_db()  # TODO: there needs to be a check to determine if the
                        # database connection was successful or not.
    globalvars.dbHandle = dbParams["handle"]
    globalvars.dbCollection = dbParams["collection_name"]

    # PROCESS ALL RECORDS
    for row in globalvars.adminList:

        arrangementInfo = {}

        for arrangementId in range(1, numArrangementInfoCols + 1):
            arrangementInfo[arrangementInfoTags[arrangementId]] = row[arrangementId - 1]

        print_info("Arrangement Info Data: {}".format(arrangementInfo))

        # function to store the data in csv
        adminStatus = adminRecord(arrangementInfo)

def errorCSV():
    # WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
    if len(globalvars.adminerrorList) > 0:
        errorsCSVFileName = ("admin_profile_errors_" + strftime("%Y-%m-%d_%H%M%S", localtime(time())) + ".csv")

        try:
            errorsCSVFileHandle = open(errorsCSVFileName, 'w')
        except IOError as ioErrorCsvWrite:
            print_error(ioErrorCsvWrite)
            print_error(errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["message"])
            exit (errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["code"])

        csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"', lineterminator='\n')

        for row in globalvars.adminerrorList:
            csvWriter.writerow(row)

        errorsCSVFileHandle.close()
        print_error("Errors were encountered and has been written to the following file: {}.".format(errorsCSVFileName))

def defineCommandLineOptions():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    argParser = argparse.ArgumentParser(description="Migrate Files for Preservation")
    argParser.add_argument('-f', '--file', nargs=1, default=False, metavar='CSVPATH', help='CSVPATH is the path to the CSV file to be used with the -f option.')
    argParser.add_argument('-q', '--quiet', action='store_true', help='Enable this option to suppress all logging, except critical error messages.')
    return argParser

def parseCommandLineArgs(argParser, args):
    parsedArgs = argParser.parse_args(args)

    if len(args) == 0:
        print_error(errorcodes.ERROR_INVALID_ARGUMENT_STRING["message"])
        argParser.print_help()
        exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING["code"])

    globalvars.quietMode = parsedArgs.quiet

    if parsedArgs.file:
        globalvars.batchMode = True
        globalvars.csvFile = parsedArgs.file[0]
    else:
        print_error(errorcodes.ERROR_FILE_ARGUMENT["message"])
        globalvars.adminerrorList.append([errorcodes.ERROR_FILE_ARGUMENT["message"]])
        errorCSV()
        exit(errorcodes.ERROR_FILE_ARGUMENT["code"])

def adminRecord(arrangementInfo):
    """adminRecord(): Adds the admin details to the existing record.

    Arguments:
        [1] arrangementInfo: dictionary containing the details to be added to the record.

    """
    query = []
    for label in arrangementInfo:
        if 'Label' in label:
            query.append({".".join([globalvars.labels.admn_entity.name, globalvars.labels.arrangement.name, label]) : arrangementInfo[label]})

    records = globalvars.dbHandle[globalvars.dbCollection].find({'$and' : query})
    records = [record for record in records]

    if(len(records) > 0):
        for document in records:
            id = document['_id']

            for keys in arrangementInfo:
                if keys in document['admin']['arrangement']:
                    flag = True
                else:
                    flag = False
                    print_info("The following record has been initialized: {}".format(arrangementInfo))
                    document['admin']['arrangement'].update(arrangementInfo)
                    dbUpdatePremisProfile = updateRecordInDB(id, document)
                    exit()

            if flag == True:
                globalvars.adminerrorList.append([errorcodes.ERROR_ADMIN_UPDATED["message"]])
                print_error(errorcodes.ERROR_ADMIN_UPDATED["message"])
                errorCSV()
                exit(errorcodes.ERROR_ADMIN_UPDATED["code"])

    else:
        globalvars.adminerrorList.append([errorcodes.ERROR_CANNOT_FIND_DOCUMENT["message"]])
        print_error(errorcodes.ERROR_CANNOT_FIND_DOCUMENT["message"])
        errorCSV()
        exit(errorcodes.ERROR_CANNOT_FIND_DOCUMENT["code"])

if __name__ == "__main__":
    main()
