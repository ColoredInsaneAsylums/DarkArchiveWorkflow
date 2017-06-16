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
# Creator: Nitin Verma
# 

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
from bson.objectid import ObjectId
from datetime import datetime
from time import localtime, time, strftime
import json
import urllib
import argparse


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

checksumAlgo = "MD5 Hash"

# DATABASE VARIABLES
DBNAME = "cshdb" # TODO: Move this to a config file, along with other db stuff
dbHandle = None # Stores the handle to access the database. Initialized to None.
dbCollection = None

# LABEL DICTIONARIES
labelDict = dict()

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


# FUNCTION DEFINITIONS 

def print_info(*args):
    """print_info(): Prints informational messages on stdout

    Arguments:
        [1] Variable length list of arguments
    
    If quiet mode is enabled, this function does nothing. If quiet mode is 
    not enabled (default) then the arguments are passed one by one to the 
    built-in print() function.
    """
    if quietMode == False:
        print(strftime("%Y-%m-%d_%H:%M:%S", localtime(time())) + ": ", end='')
        for arg in args:
            print(arg, end='')
        print()


def print_error(*args):
    """print_error():

    Arguments:
        [1]: Variable length list of arguments

    Forces print output to stderr.
    """
    for arg in args:
        print(arg, end='', file=sys.stderr)


def getFileChecksum(filePath):
    return hashlib.md5(open(filePath, 'rb').read()).hexdigest() # TODO: include this inline in the caller. remove function.


def init_db():
    """init_db():

    Arguments:
        none

    Reads the DB Configuration file, creates a connection to the database, 
    and returns a handle to the connected database
    """
    
    try:
        dbConfigJson = open("dbconf.json", "r").read()
    except IOError as exception:
        print_error(exception)
        print_error("\nCould not read the DB Configuration file 'dbconf.json'")
        quit(ERROR_CANNOT_READ_DBCONF_FILE)

    dbConfig = json.loads(dbConfigJson)
    dbAddr = dbConfig['dbaddress']
    dbUser = dbConfig['dbuser']
    #dbPass = urllib.quote_plus(dbConfig['dbpassword'])
    dbPass = dbConfig['dbpassword']
    dbName = dbConfig['dbname']
    dbCollection = dbConfig['dbcollection']

    try:
        handle = MongoClient(dbAddr)[dbName]
    except pymongo.errors.ConnectionFailure as ExceptionConnFailure:
        print_error(ExceptionConnFailure)
        exit(ERROR_CANNOT_CONNECT_TO_DB)

    try:
        handle.authenticate(dbUser, dbPass)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        exit(ERROR_CANNOT_AUTHENTICATE_DB_USER)

    dbParamsDict = dict()
    dbParamsDict["handle"] = handle
    dbParamsDict["collection_name"] = dbCollection

    return dbParamsDict


def read_label_dictionary():
    """read_label_dictionary()

    Arguments:
        None

    This function reads the file 'labels.json' to populate the label dictionary.
    This label dictionary will be used to assign labels to metadata items to be 
    recorded into a database for each transfer.
    """

    try:
        labelsJson = open("labels.json", "r").read()
    except IOError as jsonReadException:
        print_error(jsonReadException)
        print_error("\nCould not read the labels file 'labels.json'")
        quit(ERROR_CANNOT_READ_LABELS_FILE)

    try:
        labels = json.loads(labelsJson)
    except json.JSONDecodeError as jsonDecodeError:
        print_error(jsonDecodeError)
        print_error("The file 'labels.json' is not a valid JSON file. Please check the file for formatting errors.")
        exit(ERROR_INVALID_JSON_FILE)

    return labels


def insertRecordInDB(srcPath, uniqueId, dstPath, checksum, eadInfo, timestamp, csAlgo, eventType):
    """insertRecordInDB

    Arguments:
        srcPath: path to the source file
        dstPath: path to the destination file
        eadInfo: series/subseries/itemgroup/itemsubgroup info

    This function creates a database entry pertaining to the file being transferred.
    
    """
    
    record = {}
    record["_id"] = uniqueId  # This label cannot be customized as 
                              # it is needed by MongoDB for indexing purposes. If we 
                              # change this to something else, then MongoDB will add 
                              # a field named "_id" on its own with a unique ID.
    record[labelDict["preservation_info_label"]] = {
        labelDict["type_of_event_label"]: eventType,
        labelDict["source_directory"]: srcPath,
        labelDict["destination_directory"]: dstPath,
        labelDict["checksum_value"]: checksum,
        labelDict["checksum_algorithm"]: checksumAlgo,
        labelDict["file_transfer_timestamp"]: timestamp
    }

    record[labelDict["archival_info_label"]] = {}

    for eadTag in eadInfo:
        record[labelDict["archival_info_label"]][eadTag] = eadInfo[eadTag]
    
    print_info("Inserting the following record into the DB: {}".format(record))

    try:
        dbInsertResult = dbHandle[dbCollection].insert_one(record)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        return(ERROR_CANNOT_INSERT_INTO_DB)
    
    return(str(dbInsertResult.inserted_id))


def DeleteRecordFromDB(id):
    retVal = dbHandle[dbCollection].delete_one({'_id': id})
    
    if retVal.deleted_count != 1:
        print_error("Cannot remove record from DB")
        exit(ERROR_CANNOT_REMOVE_RECORD_FROM_DB)


def transfer_files(src, dst, eadInfo):
    """transfer_files(): Carries out the actual transfer of files.
    
    Arguments: 
        [1] Source - path to source directory; 
        [2] Destination - path to destination directory.
    
    Returns:
        True:
        False: 
    """
    returnData = {}  # This dict will be returned to the caller. The 'status' 
                     # element of this dict would be a binary value (True, or
                     # False) indicating success or failure, and the 'comment' 
                     # element would be a string specifying "Success" in case
                     # the transfers were successful, OR a string describing 
                     # what went wrong.

    src = os.path.abspath(src)
    dst = os.path.abspath(dst)  # Convert the source and destination paths to absolute paths.
                                # While this is not important as far as the file
                                # movement is concerned (i.e., via the shutil functions),
                                # but this is important from the metadata point-of-view.

    try:
        # Create a list of files with the given extension within the src 
        # directory.
        fileList = sorted(glob.glob(os.path.join(src, "*."+ext)))
        totalNumFiles = len(fileList)
        numFilesTransferred = 0  # Keeps track of number of files successfully
                                 # transferred.
        
        if totalNumFiles == 0:  # That no file with the extension ext was 
                                # found is an 'anomalous' condition and should
                                # be treated as an unsuccessful transfer just
                                # to caution the user. This cautioning will be
                                # very helpful in cases of large batch files
            returnData['status'] = False
            print_error("No files found with extension '{}'!".format(ext))
            returnData['comment'] = "No files found with extension '{}'!".format(ext)
            return returnData
            
        # Loop over all files with the extension ext
        for fileName in fileList:
            srcFileExt = os.path.basename(fileName).split('.')[-1]

            uniqueId = str(ObjectId())  # This generates a unique 12-byte
                                         # hexadecimal id using the BSON module.

            # Create the unique destination file path using the dst (destination
            # directory), and the uniqueId generated using ObjectId()
            dstFileUniquePath = os.path.join(dst, uniqueId + "." + srcFileExt)
            
            # Calculate the checksum for the source file. This will be used
            # later to verify the contents of the file once it has been copied
            # or moved to the destination directory
            srcChecksum = getFileChecksum(fileName)

            if move == True:
                eventType = "migration"
            else:
                eventType = "replication"
            

            currentTimeStamp = datetime.now().strftime('%d %b %Y %H:%M:%S')

            # Insert the record into the DB first, and THEN copy/move the file.
            dbRetValue = insertRecordInDB(fileName, uniqueId, dstFileUniquePath, srcChecksum, eadInfo, currentTimeStamp, checksumAlgo, eventType)
            if dbRetValue != uniqueId:
                print_error("DB Insert operation not successful. Unique ID returned by DB does not match the one provided by the script. Exiting.")
                returnData['status'] = False
                returnData['comment'] = "DB Insert operation not successful."
                return(returnData)

            # To be conservative about the transfers, this script implements the move operation as:
            # 1. COPY the file from source to destination.
            # 2. Compare the checksum of the copied file to that of the original.
            # 3. DELETE the copied file in case the checksums do not match.
            # 4. DELETE the original file in case the checksums match.
            print_info("{} '{}' from '{}' to '{}'".format("Moving" if move == True else "Copying", os.path.basename(fileName), src, dst))
            
            # Make a copy of the source file at the destination path
            shutil.copy2(fileName, dstFileUniquePath)
            
            # Calculate the checksum for the file once copied to the destination.
            dstChecksum = getFileChecksum(dstFileUniquePath)

            # Compare the checksums of the source and destination files to 
            # verify the success of the transfer. If checksums do not match,
            # it means that something went wrong during the transfer. In the 
            # case of such a mismatch, we remove the destination file, and the corresponding
            # DB record.
            if dstChecksum != srcChecksum:
                print_error("Checksum mismatch for '{}', and '{}'".format(fileName, dstFileUniquePath))

                # Remove the destination file
                try:
                    os.remove(dstFileUniquePath)
                except os.error as ExceptionFileRemoval:
                    print_error(ExceptionFileRemoval)
                    exit(ERROR_CANNOT_REMOVE_FILE)

                # Remove entry from DB
                DeleteRecordFromDB(uniqueId)

                returnData['status'] = False
                returnData['comment'] = "Checksum mismatch for '{}', and '{}'. Aborted transfers for remaining files in directory.".format(fileName, dstFileUniquePath)
                return returnData  # Something went wrong, return False
            else:
                if move == True:
                    try:
                        os.remove(dstFileUniquePath)
                    except os.error as ExceptionFileRemoval:
                        print_error("Cannot remove file from source after the move. Only a copy was made to the destination.")
                        print_error(ExceptionFileRemoval)
                        exit(ERROR_CANNOT_REMOVE_FILE)

            numFilesTransferred += 1

    except Exception as shutilException:  # Catching top-level exception to simplify the code.
        print_error(shutilException)
        print_error("Cannot complete transfer for '{}', and '{}'".format(src, dst))
        returnData.append(False)
        commentString = "Error: " + shutilException
        returnData.append(commentString)
        return returnData  # Something went wrong, return False
        
    returnData['status'] = True
    commentString = "Success. {} out of {} files transferred".format(numFilesTransferred, totalNumFiles)
    returnData['comment'] = commentString
    return returnData  # Transfers were successfully completed, return True



#PARSE AND VALIDATE COMMAND-LINE OPTIONS
argParser = argparse.ArgumentParser(description="Migrate Files for Preservation")
argParser.add_argument('-e', '--extension', nargs=1, default='*', help='Specify file EXTENSION for files that need to be migrated.')
#argParser.add_argument('srcDstPair', nargs='*', metavar='SRC DST', help='Migrate files from SRC to DST. DST will be created if it does not exist. These arguments will be ignored if the -f option is specified.')
argParser.add_argument('-f', '--file', nargs=1, default=False, metavar='CSVPATH', help='CSVPATH is the path to the CSV file to be used with the -f option.')
argParser.add_argument('-q', '--quiet', action='store_true', help='Enable this option to suppress all logging, except critical error messages.')
argParser.add_argument('-m', '--move', action='store_true', help='Enable this option to move the files instead of copying them.')

args = argParser.parse_args()

if len(sys.argv) < 2:
    argParser.print_help()
    exit(ERROR_INVALID_ARGUMENT_STRING)

ext = args.extension[0]
quietMode = args.quiet
move = args.move

if args.file:
    batchMode = True
    csvFile = args.file[0]
else:
    batchMode = False
    if len(args.srcDstPair) != 2:
        src = args.srcDstPair[0]
        dst = args.srcDstPair[1]
        transferList.append([src, dst])
    else:
        argParser.print_help()
        exit(ERROR_INVALID_ARGUMENT_STRING)
   
print_info("Extension: {}".format(ext))

if move == True:
    print_info("'move' option selected\nCAUTION: Files will be moved rather \
than copied")

print_info("quiet mode: ", quietMode)

# POPULATE LIST OF SOURCE-DESTINATION PAIRS
if batchMode == True:  # Batch mode. Read and validate CSV file.
    # Read CSV file contents into transferList.
    try:
        # Open the CSV file in read-only mode.
        csvFileHandle = open (csvFile, "r")
    except IOError as ioErrorCsvRead:
        print_error(ioErrorCsvRead)
        print_error("Could not open CSV file '{}'".format(csvFile))
        exit(ERROR_CANNOT_OPEN_CSV_FILE)
    
    # CSV file successfully opened.
    csvReader = csv.reader(csvFileHandle)  # Create an iterable object from the
                                        # CSV file using csv.reader().
    
    # Extract the first row to check if it is a header.
    firstRow = next(csvReader, None)
    firstRowPresent = True

    if firstRow == None:  # This also serves as a check for an empty CSV file
        print("The header row is invalid")
        exit(ERROR_INVALID_HEADER_ROW)

    print("Checking the header row. Header: {}".format(firstRow))
    for col in firstRow:
        if col.lower() in ['source', 'destination'] or col.startswith('ead:'):
            continue
        else:
            firstRowPresent = False
            break

    if firstRowPresent == False:
        print("The header row is invalid")
        exit(ERROR_INVALID_HEADER_ROW)


    # Extract EAD info from header row
    numEADCols = 0
    EADTags = {}
    for col in firstRow:
        if col.startswith('ead:'):
            numEADCols += 1
            EADTags[numEADCols] = col.split(':')[-1]

    minNumCols += numEADCols
    errorList.append(firstRow + ["Comments"])
    # This for loop reads and checks the format (i.e., presence of at least two
    # columns per row) of the CSV file, and populates 'transferList' which will 
    # be used for the actual file transfers.
    # 
    # FORMAT RULES/ASSUMPTIONS for the CSV file:
    #   1. The FIRST column specifies SOURCE path
    #   2. The SECOND column specifies DESTINATION path
    #   3. The remaining columns must be named like "ead:<EAD field/tag>", 
    #      e.g., "ead:series", "ead:sub-series", etc.
    rowNum = 1
    for row in csvReader:
        if len(row) < minNumCols:  # Check if the row has AT LEAST minNumCols elements.
            print_error("Row number {} in {} is not a valid input. This row will not be processed.".format(rowNum, csvFile))
            emptyStrings = ["" for i in range(0, minNumCols - len(row) - 1)]  # To align the error message to be under "Comments"
            errorList.append(row + emptyStrings + ["Not a valid input"])
        else: 
            transferList.append(row)
        rowNum += 1

    csvFileHandle.close()  # Close the CSV file as it will not be needed
                        # from this point on.

print_info("Number of directories to transfer: {}".format(len(transferList)))

'''
for row in transferList:
    print_info(row)
'''

# READ-IN THE LABEL DICTIONARY
labelDict = read_label_dictionary()
print_info("The following labels will be used for labeling metadata items in the database records:")
for key in labelDict:
    print_info(key, ":", labelDict[key])

# CREATE DATABASE CONNECTION
dbParams = init_db()  # TODO: there needs to be a check to determine if the 
                      # database connection was successful or not.
dbHandle = dbParams["handle"]
dbCollection = dbParams["collection_name"]

# PROCESS ALL TRANSFERS
for row in transferList:
    src = row[0]
    dst = row[1]

    EADData = {}

    for eadId in range(1, numEADCols + 1):
        EADData[EADTags[eadId]] = row[eadId + 1]

    print_info("EAD Data: {}".format(EADData))

    # Check if the source directory exists
    if os.path.isdir(src) != True:  # Source directory doesn't exist.
                                    # Add row to errorList, and skip to next
                                    # row
        print_info("The source directory '{}' does not exist. \
Skipping to next transfer.".format(src))
        errorList.append(row + ["Source does not exist"])
        continue

    # Check if the destination directory exists.
    # Create it if it doesn't exist.
    if os.path.isdir(dst) != True:  # Destination directory doesn't exist
        try:
            os.makedirs(dst)  # This will create all the intermediate
                              # directories required.
        except os.error as osError:
            print_error(osError)
            print_error("cannot create destination directory {}. \
                Skipping to next transfer.")
            errorList.append(row + [str(osError)])
            continue
        
    transferStatus = transfer_files(src, dst, EADData)
    
    if transferStatus['status'] != True:
        # Something bad happened during this particular transfer.
        # Add this row to the list errorList to keep a record of it.
        # Also append diagnostic information about why the transfer was not
        # successful.
        #row.append(transferStatus['comment'])
        errorList.append(row + [transferStatus['comment']])

# WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
if len(errorList) > 1:  # Because at least the header row will always be there!
    errorsCSVFileName = ("transfer_errors_" + strftime("%Y-%m-%d_%H%M%S", 
                                                    localtime(time()))
                        + ".csv")
    
    try:
        errorsCSVFileHandle = open(errorsCSVFileName, 'w')
    except IOError as ioErrorCsvWrite:
        print_error(ioErrorCsvWrite)
        print_error("Could not write CSV file for errors encountered during \
transfers")
        exit (ERROR_CANNOT_WRITE_CSV_FILE)
        
    csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"',
                        lineterminator='\n')
    
    for row in errorList:
        csvWriter.writerow(row)
        
    errorsCSVFileHandle.close()
    print_error("Not all transfers were successful. A record of rows for which \
errors were encountered has been written to the following file: \
{}".format(errorsCSVFileName))