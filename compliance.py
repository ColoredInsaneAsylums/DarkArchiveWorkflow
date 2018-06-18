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
# File Name: compliance.py
# Description: This file contains source code for the core functionality of the
#              compliance schema workflow.
#
# Creator: Sanchit Singhal (sanchit at utexas dot edu)
# Update: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#
# IMPORT NEEDED MODULES
import csv
import sys
import shutil

from  metadatautilspkg.globalvars import *
from metadatautilspkg.errorcodes import *
from metadatautilspkg.compliancemetadatautils import *
from metadatautilspkg.dbfunctions import *

def main():
    argParser = defineCommandLineOptions()
    parseCommandLineArgs(argParser, sys.argv[1:])

    print_info("quiet mode: ", globalvars.quietMode)


    # POPULATE COMPLIANCE INFORMATION FROM FILE
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
        if len(firstRow) == 0:  # This also serves as a check for an empty CSV file
            print_error(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            globalvars.complianceErrorList.append(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            errorCSV()
            exit(errorcodes.ERROR_INVALID_HEADER_ROW["code"])

        # Extract Compliance info from header row
        numComplianceInfoCols = 0
        compliancetInfoTags = {}

        for col in firstRow:
            if col.startswith(globalvars.COMPLIANCE_INFO_MARKER):
                numComplianceInfoCols += 1
                compliancetInfoTags[numComplianceInfoCols] = col.split(':')[-1] + globalvars.COMPLIANCE_INFO_LABEL_SUFFIX

        globalvars.minNumCols += numComplianceInfoCols
        globalvars.complianceErrorList.append(firstRow + ["Comments"])
        errorCSV()

        # This for loop reads and checks the format (i.errorcodes., presence of at least two
        # columns per row) of the CSV file, and populates 'globalvars.complianceList'

        rowNum = 1
        for row in csvReader:
            if len(row) < globalvars.minNumCols:  # Check if the row has AT LEAST globalvars.minNumCols elements.
                print_error("Row number {} in {} is not a valid input. This row will not be processed.".format(rowNum, globalvars.csvFile))
                emptyStrings = ["" for i in range(0, globalvars.minNumCols - len(row) - 1)]  # To align the error message to be under "Comments"
                globalvars.complianceErrorList.append(row + emptyStrings + ["Not a valid input"])
                errorCSV()
            else:
                globalvars.complianceList.append(row)
            rowNum += 1

        csvFileHandle.close()  # Close the CSV file as it will not be needed
                            # from this point on.

    print_info("Number of series,sub-series pairs to add compliance information for: {}".format(len(globalvars.complianceList)))

    # READ-IN THE LABEL DICTIONARY
    globalvars.labels = readLabelDictionary()
    print_info(globalvars.labels)

    # READ-IN THE CONTROLLED VOCABULARY
    globalvars.vocab = readControlledVocabulary()

    # CREATE DATABASE CONNECTION
    dbParams = init_db()  # TODO: there needs to be a check to determine if the
                        # database connection was successful or not.
    globalvars.dbHandle = dbParams["handle"]
    globalvars.dbCollection = dbParams["collection_name"]

    # PROCESS ALL RECORDS
    for row in globalvars.complianceList:
        series = row[0]
        subseries = row[1]

        Complianceinfo = {}

        for complianceId in range(1, numComplianceInfoCols + 1):
            Complianceinfo[compliancetInfoTags[complianceId]] = row[complianceId + 1]

        print_info("Compliance Data for Series: {}, Sub-Series: {} - {}".format(series, subseries, Complianceinfo))

        processStatus = processRecord(series, subseries, Complianceinfo)

        if processStatus['status'] != True:
            # Something bad happened during this particular record.
            # Add this row to the list globalvars.complianceErrorList to keep a note of it.
            # Also append diagnostic information about why the process was not
            # successful.
            globalvars.complianceErrorList.append(row + [processStatus['comment']])
            errorCSV()

# WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
def errorCSV():
    # WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
    if len(globalvars.complianceErrorList) > 1:
        errorsCSVFileName = ("compliance_profile_errors_" + strftime("%Y-%m-%d_%H%M%S", localtime(time())) + ".csv")

        try:
            errorsCSVFileHandle = open(errorsCSVFileName, 'w')
        except IOError as ioErrorCsvWrite:
            print_error(ioErrorCsvWrite)
            print_error(errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["message"])
            exit (errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["code"])

        csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"', lineterminator='\n')

        for row in globalvars.complianceErrorList:
            csvWriter.writerow(row)

        errorsCSVFileHandle.close()
        print_error("Errors were encountered and has been written to the following file: {}.".format(errorsCSVFileName))

def defineCommandLineOptions():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    argParser = argparse.ArgumentParser(description="Add Compliance Information to documents")
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
        globalvars.batchMode = False

def processRecord(series, subseries, Complianceinfo):
    """processRecord(): Builds the metadata profile and updates the DB

    Arguments:
        [1] series;
        [2] sub-series;
        [3] compliance information that needs to be added for that record.

    Returns:
        True:
        False:
    """
    returnData = {}  # This dict will be returned to the caller. The 'status'
                     # element of this dict would be a binary value (True, or
                     # False) indicating success or failure, and the 'comment'
                     # element would be a string specifying "Success" in case
                     # the processes were successful, OR a string describing
                     # what went wrong.

    try:
        seriesLabel =  ".".join([globalvars.labels.admn_entity.name, globalvars.labels.arrangement.name, globalvars.labels.seriesLabel.name])
        sub_seriesLabel = ".".join([globalvars.labels.admn_entity.name, globalvars.labels.arrangement.name, globalvars.labels.sub_seriesLabel.name])

        records = globalvars.dbHandle[globalvars.dbCollection].find({seriesLabel: series, sub_seriesLabel: subseries})
        records = [record for record in records]

        if(series == ""):
            records = globalvars.dbHandle[globalvars.dbCollection].find({sub_seriesLabel: subseries})
            records = [record for record in records]

        if(subseries == ""):
            records = globalvars.dbHandle[globalvars.dbCollection].find({seriesLabel: series})
            records = [record for record in records]

        if(len(records) != 0):
            for rec in records:
                if "compliance" in rec:
                    print_error(errorcodes.ERROR_COM_UPDATED["message"])
                    globalvars.complianceErrorList.append([errorcodes.ERROR_COM_UPDATED["message"]])
                    errorFlag = True

                elif "_id" in rec:
                    id = rec['_id']

                    metadataRecord = createComplianceProfile()

                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_record_type.name] = Complianceinfo["recordTypeLabel"]

                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_name.name] = Complianceinfo["retentionSchedule-authority-nameLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_url.name] = Complianceinfo["retentionSchedule-authority-urlLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_aff.name] = Complianceinfo["retentionSchedule-authority-affiliationLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_rt_init_event.name] = Complianceinfo["retentionSchedule-initiatingEventLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_rt_duration.name] = Complianceinfo["retentionSchedule-durationLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_url.name] = Complianceinfo["retentionSchedule-urlLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_retention_schedule.name][globalvars.labels.com_eff_date.name] = Complianceinfo["retentionSchedule-effectiveDateLabel"]

                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_name.name] = Complianceinfo["disposition-authority-nameLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_url.name] = Complianceinfo["disposition-authority-urlLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_authority.name][globalvars.labels.com_auth_aff.name] = Complianceinfo["disposition-authority-affiliationLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_disp_method.name] = Complianceinfo["disposition-methodLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_url.name] = Complianceinfo["disposition-urlLabel"]
                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_disposition.name][globalvars.labels.com_eff_date.name] = Complianceinfo["disposition-effectiveDateLabel"]

                    metadataRecord[globalvars.labels.com_entity.name][globalvars.labels.com_access.name][globalvars.labels.com_access_demo.name] = Complianceinfo["access-demographicLabel"]

                    dbUpdateComProfile = updateRecordInDB(id, metadataRecord)

                else:
                    print_error("Cannot complete process for '{}', and '{}'".format(series, subseries))
                    returnData['status'] = False
                    commentString = "Process is not completed"
                    returnData['comment'] = commentString
                    return returnData  # Something went wrong, return False

        else:
            print_error("Cannot complete process for '{}', and '{}'".format(series, subseries))
            returnData['status'] = False
            commentString = "Process is not completed"
            returnData['comment'] = commentString
            return returnData  # Something went wrong, return False

        if errorFlag == True:
            errorCSV()
        else:
            errorFlag = False

    except Exception as shutilException:  # Catching top-level exception to simplify the code.
        print_error(shutilException)
        print_error("Cannot complete process for '{}', and '{}'".format(series, subseries))
        print_error(shutilException)
        returnData['status'] = False
        commentString = "Error: " + shutilException
        returnData['comment'] = commentString
        return returnData  # Something went wrong, return False

    returnData['status'] = True
    commentString = "Successfully completed process"
    returnData['comment'] = commentString
    return returnData  # Transfers were successfully completed, return True

if __name__ == "__main__":
    main()
