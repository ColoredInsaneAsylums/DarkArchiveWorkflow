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

# ERROR CODES
ERROR_INVALID_ARGUMENT_STRING = -1
ERROR_CANNOT_OPEN_CSV_FILE = -2
ERROR_CANNOT_WRITE_CSV_FILE = -3
ERROR_CANNOT_READ_DBCONF_FILE = -4
ERROR_INVALID_HEADER_ROW = -5


# FUNCTION DEFINITIONS 

def print_usage():
    """print_usage(): Prints usage instruction(s).
    
    Arguments: none.
    """
    
    print("usage: {} [OPTION]... SOURCE DEST".format(sys.argv[0]))
    print("       {} [OPTION]... -f CSV".format(sys.argv[0]))


def print_help():
    """print_help(): Prints detailed help information about using the script.
    
    Arguments: none.
    """
    print_usage()
    print()
    print("{} is a file transfer script that can be used to transfer \
entire contents of a source directory to a destination directory.\
".format(sys.argv[0]))
    print("By default (with no command line options specified) all files from \
SOURCE are COPIED to DEST.")
    print("In the batch mode (option -f) the script can handle a batch of \
transfers by specifying source-destination pairs in a CSV file.")
    print("If a particular source-destination pair cannot be processed for \
some reason, that pair will be written to a CSV file located at the path \
from which the script was called.")
    print()
    print("OPTIONS:")
    print("  -e: The option to specify a particular file-extension to be \
transferred. e.g., tif, jpg, pdf, etc. The file-extension should be passed as \
an argument to this option.")
    print("  -f: To specify batch mode. This option takes a file path as \
argument. The file should be a CSV file, with a minimum of TWO columns -- \
first column specifying a SOURCE directory, and the second column specifying \
a DESTINATION directory. All extra columns will be ignored.")
    print("  -h: Prints this help information.")
    print("  -m: When specified, this option will MOVE files from a source \
directory to the destination directory instead of copying them. Use with \
caution!")
    print("  -q: Quiet mode. Specifying this option quiets all the \
informational prints. Only error messages and critical information will be \
printed")


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

    handle = MongoClient(dbAddr)[dbName]
    handle.authenticate(dbUser, dbPass)
    return [handle, dbCollection]


def insertRecordInDB(srcPath, uniqueId, dstPath, checksum, eadInfo, timestamp, csAlgo, eventType):
    """insertRecordInDB

    Arguments:
        srcPath: path to the source file
        dstPath: path to the destination file
        eadInfo: series/subseries/itemgroup/itemsubgroup info

    This function creates a database entry pertaining to the file being transferred.
    
    """
    
    record = {}
    record["_id"] = uniqueId
    record["preservationInfo"] = {
        "eventType": eventType,
        "source": srcPath,
        "destination": dstPath,
        "checksum": checksum,
        "checksumAlgo": checksumAlgo,
        "timestamp": timestamp
    }

    record["EADInfo"] = {}
    for eadTag in eadInfo:
        record["EADInfo"][eadTag] = eadInfo[eadTag]
    
    dbInsertResult = dbHandle[dbCollection].insert_one(record)
    return(str(dbInsertResult.inserted_id))

def transfer_files(src, dst, eadInfo):
    """transfer_files(): Carries out the actual transfer of files.
    
    Arguments: 
        [1] Source - path to source directory; 
        [2] Destination - path to destination directory.
    
    Returns:
        True:
        False: 
    """
    returnList = []  # This list will be returned to the caller. The first 
                     # element of this list would be a binary value (True, or
                     # False) indicating success or failure, and the second 
                     # element would be a string specifying "Success" in case
                     # the transfers did not encounter any anomalous/error 
                     # condition(s), OR a string describing what went wrong.

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
            returnList.append(False)
            print_error("No files found with extension '{}'!".format(ext))
            returnList.append("No files found with extension '{}'!".format(ext))
            return returnList
            
        # Loop over all files with the extension ext
        for fileName in fileList:
            dstFilePath = os.path.join(dst, os.path.basename(fileName))
            srcFileExt = os.path.basename(fileName).split('.')[-1]

            uniqueFileName = str(ObjectId())  # This generates a unique 12-byte
                                         # hexadecimal id using the BSON module.

            # Create the unique destination file path using the dst (destination
            # directory), and the uniqueFileName generated using ObjectId()
            uniqueDstFilePath = os.path.join(dst, uniqueFileName + "." + srcFileExt)
            
            # Calculate the checksum for the source file. This will be used
            # later to verify the contents of the file once it has been copied
            # or moved to the destination directory
            srcChecksum = getFileChecksum(fileName)

            if move == True: 
                print_info("MOVING '{}' from '{}' to '{}'".format(os.path.basename(fileName),
                            src, dst))
                # MOVE files from src to dst
                shutil.move(fileName, uniqueDstFilePath)
            else:
                print_info("COPYING '{}' from '{}' to '{}'".format(os.path.basename(fileName),
                        src, dst))
                # COPY files (with metadata) from src to dst
                shutil.copy2(fileName, uniqueDstFilePath)
            
            # Calculate the checksum for the file once copied/moved to the
            # destination.
            dstChecksum = getFileChecksum(uniqueDstFilePath)

            # Compare the checksums of the source and destination files to 
            # verify the success of the transfer. If checksums do not match,
            # it means that something went wrong during the transfer. In the 
            # case of such a mismatch, we resort to the same strategy as for 
            # shutil exceptions below. i.e., we create an error report string
            # and return it to the caller.
            if dstChecksum != srcChecksum:
                print_error("Checksum mismatch for '{}', and '{}'".format(fileName, uniqueDstFilePath))
                returnList.append(False)
                commentString = "Error: Checksum mismatch!"
                returnList.append(commentString)
                return returnList  # Something went wrong, return False

            currentTimeStamp = datetime.now().strftime('%d %b %Y %H:%M:%S') #TODO: get this timestamp from the file's metadata (stat?)

            # Finally, if control reaches to this point, it means that the
            # current transfer was successful, and that now we are ready to
            # create an entry in the Mongo database.

            if move == True:
                eventType = "migration"
            else:
                eventType = "replication"
            insertRecordInDB(fileName, uniqueFileName, uniqueDstFilePath, dstChecksum, eadInfo, currentTimeStamp, checksumAlgo, eventType) # uniqueFileName should be renamed because it's purpose is to carry the unique ID rather than the file name

            numFilesTransferred += 1

    except Exception as shutilException:  # Catching top-level exception to
                                          # simplify the code.
        print_error(shutilException)
        print_error("Cannot complete transfer for '{}', and '{}'".format(src, dst))
        returnList.append(False)
        commentString = "Error: " + shutilException
        returnList.append(commentString)
        return returnList  # Something went wrong, return False
        
    returnList.append(True)
    commentString = "Success. {} out of {} files transferred".format(numFilesTransferred, totalNumFiles)
    returnList.append(commentString)
    return returnList  # Transfers were successfully completed, return True



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
    
    # Check if the first row is a header row.
    firstRow = next(csvReader, None)
    firstRowPresent = True

    if firstRow == None:
        print("The header row is invalid")
        exit(ERROR_INVALID_HEADER_ROW)

    print("checking the header row. Header: {}".format(firstRow))
    for col in firstRow:
        if col in ['Source', 'Destination'] or col.startswith('ead:'):
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

    minNumCols = minNumCols + numEADCols
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
        #print(len(row), " ", row[0:6])
        #continue
        try:  # 'Try' to capture the first minNumCols columns (i.e. row[0] to row[5])
            # into transferList.
            #transferList.append([row[0], row[1]])
            #transferList.append(row[0:minNumCols])
            transferList.append(row)
        except IndexError as indexError:  # If there are not AT LEAST 6 columns
                                        # in the row, append the row as it is
                                        # (i.e. w/o indexing cols 0 thru 5).
                                        #
                                        # Will be handled while processing
                                        # transfers.
            #transferList.append(row)
            print_error(indexError)
            print_error("Row number {} in {} is not a valid input. This row will \
not be processed.".format(rowNum, csvFile))
            errorList.append(row + ["Not a valid input"])
        rowNum += 1

    csvFileHandle.close()  # Close the CSV file as it will not be needed
                        # from this point on.

print_info("Number of directories to transfer: {}".format(len(transferList)))

for row in transferList:
    print_info(row)

# CREATE DATABASE CONNECTION
dbParams = init_db() # TODO: there needs to be a check to determine if the 
                    # database connection was successful or not.
dbHandle = dbParams[0]
dbCollection = dbParams[1]

# PROCESS ALL TRANSFERS
for row in transferList:
    if len(row) >= minNumCols:  # We need AT LEAST this many columns. Any extra column(s) will
                    # be ignored.

        # The next six lines require the CSV file to be in the specific format.
        src = row[0]
        dst = row[1]

        EADData = {}

        for eadNum in range(1, numEADCols + 1):
            EADData[EADTags[eadNum]] = row[eadNum + 1]

        print_info("\nAssessing the following directories for next transfer:")
        print_info("EAD Data: {}".format(EADData))
        #print_info("itemgroup: {}, itemsubgroup: {}".format(itemgroup, itemsubgroup))
    else:
        print_info("Transfer for '{}' not possible. Skipping to next \
transfer.".format(row))
        # Append the list row (with less than 2 elements) to errorList, and skip
        # to the next row in transferList
        errorList.append(row)
        continue

    # Check if src and dst exist
    if os.path.isdir(src) != True:  # Source directory doesn't exist.
                                    # Add row to errorList, and skip to next
                                    # row
        print_info("the source directory '{}' does not exist. \
Skipping to next transfer.".format(src))
        errorList.append(row)
        continue
    elif os.path.isdir(dst) != True:  # Destination directory doesn't exist
        try:
            os.makedirs(dst)  # This will create all the intermediate
                            # directories required.
        except os.error as osError:
            print_error(osError)
            print_error("cannot create destination directory {}. \
                Skipping to next transfer.")
            errorList.append(row)
            continue
        
    transferStatus = transfer_files(src, dst, EADData)
    
    if transferStatus[0] != True:
        # Something bad happened during this particular transfer.
        # Add this row to the list errorList to keep a record of it.
        # Also append diagnostic information about why the transfer was not
        # successful.
        row.append(transferStatus[1])
        errorList.append(row)
        

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