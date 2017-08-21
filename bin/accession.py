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
# File Name: accession.py
# Description: This file contains source code for the core functionality of the 
#              archival accessioning workflow.
#
# Creator: Nitin Verma (nitin dot verma at utexas dot edu)
#

# IMPORT NEEDED MODULES
import csv
import sys
import os
import glob
import shutil

import metadatautilspkg.globalvars as globalvars
import metadatautilspkg.errorcodes as errorcodes
from metadatautilspkg.metadatautils import *
from metadatautilspkg.dbfunctions import *
from metadatautilspkg.premis import *
from metadatautilspkg.adminmetadatautils import *


def main():
    argParser = defineCommandLineOptions()
    parseCommandLineArgs(argParser, sys.argv[1:])

    print_info("Extension: {}".format(globalvars.ext))

    if globalvars.move == True:
        print_info("'move' option selected\nCAUTION: Files will be moved rather than copied")

    print_info("quiet mode: ", globalvars.quietMode)

    # POPULATE LIST OF SOURCE-DESTINATION PAIRS
    if globalvars.batchMode == True:  # Batch mode. Read and validate CSV file.
        # Read CSV file contents into globalvars.transferList.
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
        if firstRow == None or isHeaderValid(firstRow) == False:  # This also serves as a check for an empty CSV file
            print_error(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            exit(errorcodes.ERROR_INVALID_HEADER_ROW["code"])

        # Extract Arrangement info from header row
        numArrangementInfoCols = 0
        arrangementInfoTags = {}

        for col in firstRow:
            if col.startswith(globalvars.ARRANGEMENT_INFO_MARKER):
                numArrangementInfoCols += 1
                arrangementInfoTags[numArrangementInfoCols] = col.split(':')[-1] + globalvars.ARRANGEMENT_INFO_LABEL_SUFFIX

        globalvars.minNumCols += numArrangementInfoCols
        globalvars.errorList.append(firstRow + ["Comments"])
        # This for loop reads and checks the format (i.errorcodes., presence of at least two
        # columns per row) of the CSV file, and populates 'globalvars.transferList' which will
        # be used for the actual file transfers.
        #
        # FORMAT RULES/ASSUMPTIONS for the CSV file:
        #   1. The FIRST column specifies SOURCE path
        #   2. The SECOND column specifies DESTINATION path
        #   3. The remaining columns must be named like "arrange:<Arrange Info Field/Tag>",
        #      errorcodes.globalvars., "arrange:series", "ead:sub-series", etc.
        rowNum = 1
        for row in csvReader:
            if len(row) < globalvars.minNumCols:  # Check if the row has AT LEAST globalvars.minNumCols elements.
                print_error("Row number {} in {} is not a valid input. This row will not be processed.".format(rowNum, globalvars.csvFile))
                emptyStrings = ["" for i in range(0, globalvars.minNumCols - len(row) - 1)]  # To align the error message to be under "Comments"
                globalvars.errorList.append(row + emptyStrings + ["Not a valid input"])
            else:
                globalvars.transferList.append(row)
            rowNum += 1

        csvFileHandle.close()  # Close the CSV file as it will not be needed
                            # from this point on.

    print_info("Number of directories to transfer: {}".format(len(globalvars.transferList)))

    # READ-IN THE LABEL DICTIONARY
    globalvars.labels = readLabelDictionary()
    print_info("The following labels will be used for labeling metadata items in the database records:")
    #for key in globalvars.labels:
        #print_info(key, ":", globalvars.labels[key])
    print_info(globalvars.labels)

    # READ-IN THE CONTROLLED VOCABULARY
    globalvars.vocab = readControlledVocabulary()

    # CREATE DATABASE CONNECTION
    dbParams = init_db()  # TODO: there needs to be a check to determine if the 
                        # database connection was successful or not.
    globalvars.dbHandle = dbParams["handle"]
    globalvars.dbCollection = dbParams["collection_name"]

    # PROCESS ALL TRANSFERS
    for row in globalvars.transferList:
        src = row[0]
        dst = row[1]

        arrangementInfo = {}

        for arrangementId in range(1, numArrangementInfoCols + 1):
            arrangementInfo[arrangementInfoTags[arrangementId]] = row[arrangementId + 1]

        print_info("Arrangement Info Data: {}".format(arrangementInfo))

        # Check if the source directory exists
        if os.path.isdir(src) != True:  # Source directory doesn't exist.
                                        # Add row to globalvars.errorList, and skip to next
                                        # row
            print_info("The source directory '{}' does not exist. \
    Skipping to next transfer.".format(src))
            globalvars.errorList.append(row + ["Source does not exist"])
            continue

        transferStatus = transferFiles(src, dst, arrangementInfo)

        if transferStatus['status'] != True:
            # Something bad happened during this particular transfer.
            # Add this row to the list globalvars.errorList to keep a record of it.
            # Also append diagnostic information about why the transfer was not
            # successful.
            #row.append(transferStatus['comment'])
            globalvars.errorList.append(row + [transferStatus['comment']])

    # WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
    if len(globalvars.errorList) > 1:  # Because at least the header row will always be there!
        errorsCSVFileName = ("transfer_errors_" + strftime("%Y-%m-%d_%H%M%S", localtime(time())) + ".csv")

        try:
            errorsCSVFileHandle = open(errorsCSVFileName, 'w')
        except IOError as ioErrorCsvWrite:
            print_error(ioErrorCsvWrite)
            print_error(errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["message"])
            exit (errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["code"])

        csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"', lineterminator='\n')

        for row in globalvars.errorList:
            csvWriter.writerow(row)

        errorsCSVFileHandle.close()
        print_error("Not all transfers were successful. A record of rows for which errors were encountered has been written to the following file: {}".format(errorsCSVFileName))


def transferFiles(src, dst, arrangementInfo):
    """transferFiles(): Carries out the actual transfer of files.
    
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

    # Convert the source and destination paths to absolute paths.
    # While this is not important as far as the file
    # movement is concerned (i.errorcodes., via the shutil functions),
    # but this is important from the metadata point-of-view.
    src = os.path.abspath(src)
    dst = os.path.abspath(dst)

    srcDirectory = src
    dstDirectory = dst

    # Check if the destination directory exists.
    # Create it if it doesn't exist.
    if os.path.isdir(dstDirectory) != True:  # Destination directory doesn't exist
        try:
            os.makedirs(dst)  # This will create all the intermediate
                              # directories required.
        except os.error as osError:
            print_error(osError)
            globalvars.errorList.append(row + [str(osError)])
            print_error(errorcodes.ERROR_CANNOT_CREATE_DESTINATION_DIRECTORY["message"].format(dst))
            exit(errorcodes.ERROR_CANNOT_CREATE_DESTINATION_DIRECTORY["code"])

        prevHighestSerialNo = 0  # Initialize the serial number to 1, since this
                          # destination directory has just been created.
    else:
        prevHighestSerialNo = getHighestSerialNo(srcDirectory)

    print_info("Previous highest file serial number: {}".format(prevHighestSerialNo))

    try:
        # Create a list of files with the given extension within the src 
        # directory.
        fileList = sorted(glob.glob(os.path.join(src, "*." + globalvars.ext)))
        totalNumFiles = len(fileList)
        numFilesTransferred = 0  # Keeps track of number of files successfully
                                 # transferred in the current run.

        if totalNumFiles == 0:  # That no file with the extension globalvars.ext was 
                                # found is an 'anomalous' condition and should
                                # be treated as an unsuccessful transfer just
                                # to caution the user. This cautioning will be
                                # very helpful in cases of large batch files
            returnData['status'] = False
            print_error("No files found with extension '{}'!".format(globalvars.ext))
            returnData['comment'] = "No files found with extension '{}'!".format(globalvars.ext)
            return returnData

        currentSerialNo = prevHighestSerialNo + 1
        # Loop over all files with the extension globalvars.ext
        for fileName in fileList[prevHighestSerialNo:]:
            srcFileName = os.path.basename(fileName)
            srcFileExt = srcFileName.split('.')[-1]

            # Initialize a metadata record object
            recordParams = {}
            recordParams["fileName"] = fileName
            recordParams["fileSize"] = os.path.getsize(fileName)
            recordParams["fmtName"] = getFileFormatName(srcFileName)
            recordParams["fmtVer"] = getFileFormatVersion(srcFileName)
            recordParams[globalvars.ARRANGEMENT_INFO_LABEL] = arrangementInfo

            metadataRecord = initMetadataRecord(recordParams)

            # Extract the unique id from the just-initialized record
            uniqueId = metadataRecord["_id"]

            idAssignmentEvent = createIDAssignmentEvent(uniqueId)
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(idAssignmentEvent)

            # Create the unique destination file path using the dst (destination
            # directory), and the uniqueId generated using ObjectId()
            dstFilePrelimPath = os.path.join(dst, srcFileName)
            dstFileUniquePath = os.path.join(dst, uniqueId + "." + srcFileExt)
            dstFileName = os.path.basename(dstFileUniquePath)

            # Calculate the checksum for the source file. This will be used
            # later to verify the contents of the file once it has been copied
            # or moved to the destination directory
            srcChecksum = getFileChecksum(fileName)

            msgDigestCalcEvent = createMsgDigestCalcEvent(srcChecksum, globalvars.CHECKSUM_ALGO)
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(msgDigestCalcEvent)
            # Record the checksum, and the checksum algorithm in the 'object' entity
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fixity.name][globalvars.labels.obj_msgdgst_algo.name] = globalvars.CHECKSUM_ALGO
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fixity.name][globalvars.labels.obj_msgdgst.name] = srcChecksum


            # To be conservative about the transfers, this script implements the move operation as:
            # 1. COPY the file from source to destination.
            # 2. Compare the checksum of the copied file to that of the original.
            # 3. DELETE the copied file in case the checksums do not match.
            # 4. DELETE the original file in case the checksums match.
            print_info("{} '{}' from '{}' to '{}'".format("Moving" if globalvars.move == True else "Copying", os.path.basename(fileName), src, dst))

            # Make a copy of the source file at the destination path
            shutil.copy(fileName, dstFilePrelimPath)

            if globalvars.move == True:
                eventType = "migration"
            else:
                eventType = "replication"

            fileCopyEvent = createFileCopyEvent(eventType, fileName, dstFilePrelimPath)
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(fileCopyEvent)

            # Rename the destination file
            os.rename(dstFilePrelimPath, dstFileUniquePath)
            filenameChangeEvent = createFilenameChangeEvent(dstFilePrelimPath, dstFileUniquePath)
            metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(filenameChangeEvent)

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
                    print_error(errorcodes.ERROR_CANNOT_REMOVE_FILE["message"])
                    exit(errorcodes.ERROR_CANNOT_REMOVE_FILE["code"])

                # Remove entry from DB if present
                deleteRecordFromDB(uniqueId)

                returnData['status'] = False
                returnData['comment'] = "Checksum mismatch for '{}', and '{}'. Aborted transfers for remaining files in directory.".format(fileName, dstFileUniquePath)
                return returnData  # Something went wrong, return False
            else:
                fixityCheckEvent = createFixityCheckEvent(True, dstChecksum)
                metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(fixityCheckEvent)

                metadataRecord = updateSerialNumber(metadataRecord, currentSerialNo)

                accessionEvent = createAccessionEvent()
                metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name].append(accessionEvent)
                # Insert the record into the DB first, and THEN copy/move the file.
                dbRetValue = insertRecordInDB(metadataRecord)

                if dbRetValue != uniqueId:
                    print_error("DB Insert operation not successful. Unique ID returned by DB does not match the one provided by the script. Exiting.")
                    returnData['status'] = False
                    returnData['comment'] = "DB Insert operation not successful."
                    return(returnData)

                if globalvars.move == True:
                    try:
                        os.remove(dstFileUniquePath)
                    except os.error as ExceptionFileRemoval:
                        print_error("Cannot remove file '{}' from source '{}' after the move. Only a copy was made to the destination.".format(srcFileName, srcDirectory))
                        print_error(ExceptionFileRemoval)
                        print_error(errorcodes.ERROR_CANNOT_REMOVE_FILE["message"])
                        exit(errorcodes.ERROR_CANNOT_REMOVE_FILE["code"])

                # Increment the file serial number for the next transfer
                # and the corresponding DB record
                currentSerialNo += 1

            numFilesTransferred += 1

    except Exception as shutilException:  # Catching top-level exception to simplify the code.
        print_error(shutilException)
        print_error("Cannot complete transfer for '{}', and '{}'".format(src, dst))
        print_error(shutilException)
        returnData['status'] = False
        commentString = "Error: " + shutilException
        returnData['comment'] = commentString
        return returnData  # Something went wrong, return False
        
    returnData['status'] = True
    commentString = "Success. {} out of {} files transferred".format(numFilesTransferred, totalNumFiles)
    returnData['comment'] = commentString
    return returnData  # Transfers were successfully completed, return True


if __name__ == "__main__":
    main()