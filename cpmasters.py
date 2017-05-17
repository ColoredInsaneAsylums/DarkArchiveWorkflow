# -*- coding: utf-8 -*-

# COPYRIGHT STATEMENT GOES HERE.

# CREDITS

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

# DATABASE VARIABLES
DBNAME = "cshdb" # TODO: Move this to a config file, along with other db stuff
dbHandle = None # Stores the handle to access the database. Initialized to None.
dbCollection = None

# ERROR CODES
ERROR_INVALID_ARGUMENT_STRING = -1
ERROR_CANNOT_OPEN_CSV_FILE = -2
ERROR_CANNOT_WRITE_CSV_FILE = -3
ERROR_CANNOT_READ_DBCONF_FILE = -4


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


def insertRecordInDB(srcPath, uniqueId, dstPath, checksum, eadInfo, timestamp):
    """insertRecordInDB

    Arguments:
        srcPath: path to the source file
        dstPath: path to the destination file
        eadInfo: series/subseries/itemgroup/itemsubgroup info

    This function creates a database entry pertaining to the file being transferred.
    
    """

    if move == True:
        eventType = "migration"
    else:
        eventType = "replication"
    
    record = {}
    record["_id"] = uniqueId
    record["preservationInfo"] = {
        "eventType": eventType,
        "source": srcPath,
        "destination": dstPath,
        "checksum": checksum,
        "timestamp": timestamp
    }

    record["EADInfo"] = {
        "series": eadInfo[0],
        "subseries": eadInfo[1],
        "itemgroup": eadInfo[2],
        "itemsubgroup": eadInfo[3]
    }

    dbInsertResult = dbHandle[dbCollection].insert_one(record)
    return(str(dbInsertResult.inserted_id))


def updateRecordInDB(objectId, uniqueDstFilePath, checksum, timestamp):
    
    dbHandle[dbCollection].update_one({"_id": ObjectId(objectId)}, {"$set": {"preservationInfo.destination": uniqueDstFilePath, "preservationInfo.checksum": checksum, "preservationInfo.timestamp": timestamp}})

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
            
            # Calculate the checksum for the file as copied/moved to the
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
            insertRecordInDB(fileName, uniqueFileName, uniqueDstFilePath, dstChecksum, eadInfo, currentTimeStamp) # uniqueFileName should be renamed because it's purpose is to carry the unique ID rather than the file name

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


def main():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    try:
        # gnu_getopt() returns two iterable objects. The first object contains all
        # the command-line options, and the second object contains any remaining 
        # arguments that were not associated with any option.
        options, nonOptionArguments = getopt.gnu_getopt(sys.argv[1:], "e:f:hmq")
    except getopt.GetoptError as optionError:
        print_error(optionError)
        print_usage()
        exit(ERROR_INVALID_ARGUMENT_STRING)


    # Error check: if NO option is given, then EXACTLY TWO arguments -- source and
    # destination -- are REQUIRED.
    # If the above condition is not met, then display usage info and exit.
    if len(options) == 0 and len(nonOptionArguments) != 2:
        print_usage()
        exit(ERROR_INVALID_ARGUMENT_STRING)

    # Extract options and their arguments 
    for opt, arg in options:
        if opt == '-e':  # Option to specify a particular fileName extension for 
                        # transfer. Takes one string argument. e.g. tif, or png.
            ext = arg
        elif opt == '-f':  # Option to specify a batch file (in CSV format) with
                        # rows of source-destination pairs.
                        # Takes one argument: path to the CSV file.
            batchMode = True
            csvFile = arg
        elif opt == '-h':  # Option to print help text. No argument required.
            print_help()
            exit()
        elif opt == '-m':  # If specified, files will be MOVED instead of being
                        # copied. No argument required.
            move = True
        elif opt == '-q':  # Switches off the VERBOSE mode. No argument required.
            quietMode = True

    print_info("options:", options)
    print_info("nonOptionArguments:", nonOptionArguments)

    # If batch mode is used (i.e., option -f), then there should NOT be any 
    # non-option arguments on the command-line.
    #
    # If, however, batch mode is NOT used (i.e., the option -f is not specified),
    # then there should be EXACTLY TWO non-option arguments specified on the 
    # command-line. The FIRST of those two arguments would be taken to be the 
    # source path, and the SECOND to be the destination path.
    if batchMode == True and len(nonOptionArguments) == 0:
        print_info("CSV file: {}".format(csvFile))
    elif batchMode == False and len(nonOptionArguments) == 2:
        # If one source-destination pair was supplied on the command line 
        # directly.
        src = nonOptionArguments[0]  # Source path
        dst = nonOptionArguments[1].rstrip()  # Destination path
        transferList.append([src, dst])  # Store src and dst in a list, and append
                                        # that list to transferList.
        print_info("Source: {}\nDestination: {}".format(src, dst))
    else:
        print_usage()
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
        
        # This for loop reads and checks the format (i.e., presence of at least two
        # columns per row) of the CSV file, and populates 'transferList' which will 
        # be used for the actual file transfers.
        # 
        # FORMAT RULES/ASSUMPTIONS for the CSV file:
        #   1. The FIRST column specifies SOURCE path
        #   2. The SECOND column specifies DESTINATION path
        rowNum = 1
        for row in csvReader:
            #print(len(row), " ", row[0:6])
            #continue
            try:  # 'Try' to capture the first six columns (i.e. row[0] to row[5])
                # into transferList.
                #transferList.append([row[0], row[1]])
                transferList.append(row[0:6])
            except IndexError as indexError:  # If there are not AT LEAST 6 columns
                                            # in the row, append the row as it is
                                            # (i.e. w/o indexing cols 0 thru 5).
                                            #
                                            # Will be handled while processing
                                            # transfers.
                transferList.append(row)
                print_error(indexError)
                print_error("Row number {} in {} is not a valid input. This row will \
    not be processed.".format(rowNum, csvFile))
            rowNum += 1

        errorList.append(transferList[0]) # Retain the first row
                                        # of the transferList to form the header
                                        # of the errorList.
        errorList[0].append("Comment")
        
        transferList = transferList[1:]  # Delete the first element because it
                                        # corresponds to the CSV header row.
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
        if len(row) >= 6:  # We need AT LEAST SIX columns. Any extra column(s) will
                        # be ignored.

            # The next six lines require the CSV file to be in the specific format.
            src = row[0]
            dst = row[1]
            series = row[2]
            subseries = row[3]
            itemgroup = row[4]
            itemsubgroup = row[5]

            print_info("\nAssessing the following directories for next transfer:")
            print_info("source: {}, destination: {}, series: {}, subseries: {},".format(src, dst, series, subseries))
            print_info("itemgroup: {}, itemsubgroup: {}".format(itemgroup, itemsubgroup))
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
            
        transferStatus = transfer_files(src, dst, [series, subseries, itemgroup, itemsubgroup])
        
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


if __name__ == "__main__":
    main()