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
# File Name: derivatives.py
# Description: This file contains source code for the core functionality of deriving files from the original file.
#
# Creator: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#
# IMPORT NEEDED MODULES
import csv
import sys
import re
import os

from datetime import datetime
from time import localtime, time, strftime
from subprocess import PIPE, Popen

from metadatautilspkg.globalvars import *
from metadatautilspkg.errorcodes import *
from metadatautilspkg.dbfunctions import *
from metadatautilspkg.premis import *
from metadatautilspkg.metadatautils import *

def main():

    # Verify whether ImageMagick is installed in the system or not. Throws error if ImageMagick is not installed.
    output, error, exitcode = runCmd('identify -version')
    imgversion = output.decode('utf-8').split('\n')

    # remove null values from the list
    while '' in imgversion:
        imgversion.remove('')

    imgver = []
    for str in imgversion:
            if ': ' in str:
                imgver.append(str)
            else:
                globalvars.derivativeErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"]])
                print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"])
                errorCSV()
                exit(errorcodes.ERROR_INSTALL_IMAGEMAGICK["code"])

    version = {}
    for item in imgver:
        key, value = item.split(": ")
        key = key.strip(" ")
        value = value.strip(" ")
        version[key] = value

    if 'Version' in version:
        ver = version['Version']
        if 'ImageMagick' not in ver:
            globalvars.derivativeErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"]])
            print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"])
            errorCSV()
            exit(errorcodes.ERROR_INSTALL_IMAGEMAGICK["code"])
    else:
        globalvars.derivativeErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"]])
        print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"])
        errorCSV()
        exit(errorcodes.ERROR_INSTALL_IMAGEMAGICK["code"])

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
        if len(firstRow) == 0: # This also serves as a check for an empty CSV file
            print_error(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            globalvars.derivativeErrorList.append([errorcodes.ERROR_INVALID_HEADER_ROW["message"]])
            errorCSV()
            exit(errorcodes.ERROR_INVALID_HEADER_ROW["code"])

        # This for loop reads and checks the format (i.errorcodes., presence of at least two
        # columns per row) of the CSV file, and populates 'globalvars.technicalList'

        rowNum = 1
        for row in csvReader:
            globalvars.derivativeList.append(row)
            rowNum += 1

        csvFileHandle.close()  # Close the CSV file as it will not be needed from this point on.

    print_info("Number of folder path(s) read from the CSV: {}".format(len(globalvars.derivativeList)))

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
    for row in globalvars.derivativeList:

        filePath = row[0]
        print_info("filepath Info Data: {}".format(filePath))

        if os.path.isdir(filePath) != True:
            globalvars.technicalErrorList.append([errorcodes.ERROR_CANNOT_FIND_DIRECTORY["message"].format(filePath)])
            print_error(errorcodes.ERROR_CANNOT_FIND_DIRECTORY["message"].format(filePath))
            errorCSV()
            exit(errorcodes.ERROR_CANNOT_FIND_DIRECTORY["code"])
        else:
            derivativeFile = derivativeRecord(filePath)

def errorCSV():
    # WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
    if len(globalvars.derivativeErrorList) > 0:
        errorsCSVFileName = ("derivative_profile_errors_" + strftime("%Y-%m-%d_%H%M%S", localtime(time())) + ".csv")

        try:
            errorsCSVFileHandle = open(errorsCSVFileName, 'w')
        except IOError as ioErrorCsvWrite:
            print_error(ioErrorCsvWrite)
            print_error(errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["message"])
            exit (errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["code"])

        csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"', lineterminator='\n')

        for row in globalvars.derivativeErrorList:
            csvWriter.writerow(row)

        errorsCSVFileHandle.close()
        print_error("Errors were encountered and has been written to the following file: {}.".format(errorsCSVFileName))

def defineCommandLineOptions():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    argParser = argparse.ArgumentParser(description="Migrate Files for Preservation")
    argParser.add_argument('-f', '--file', nargs=1, default=False, metavar='CSVPATH', help='CSVPATH is the path to the CSV file to be used with the -f option.')
    argParser.add_argument('-q', '--quiet', action='store_true', help='Enable this option to suppress all logging, except critical error messages.')
    argParser.add_argument('-s', '--sourcefiletype', nargs=1, default=False, metavar='SOURCEFILETYPE', help='CSVPATH is the path to the CSV file to be used with the -f option.')
    argParser.add_argument('-c', '--destfiletype', nargs=1, default=False, metavar='DESTFILETYPE', help='Enable this option to suppress all logging, except critical error messages.')
    argParser.add_argument('-r', '--resize', nargs=1, default=False, metavar='RESIZEDIM', help='CSVPATH is the path to the CSV file to be used with the -f option.')
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
        globalvars.derivativeErrorList.append([errorcodes.ERROR_FILE_ARGUMENT["message"]])
        errorCSV()
        exit(errorcodes.ERROR_FILE_ARGUMENT["code"])

    if parsedArgs.sourcefiletype:
        globalvars.sourcefiletype = parsedArgs.sourcefiletype[0]
    else:
        print_error(errorcodes.ERROR_SOURCETYPE["message"])
        globalvars.derivativeErrorList.append([errorcodes.ERROR_SOURCETYPE["message"]])
        errorCSV()
        exit(errorcodes.ERROR_SOURCETYPE["code"])

    if parsedArgs.destfiletype:
        globalvars.destfiletype = parsedArgs.destfiletype[0]
    else:
        globalvars.destfiletype = globalvars.sourcefiletype

    if parsedArgs.resize:
        globalvars.resize = parsedArgs.resize[0]

    if((globalvars.destfiletype == "") and (globalvars.resize == "")):
        print_error(errorcodes.ERROR_DESTTYPE_RESIZE["message"])
        globalvars.derivativeErrorList.append([errorcodes.ERROR_DESTTYPE_RESIZE["message"]])
        errorCSV()
        exit(errorcodes.ERROR_DESTTYPE_RESIZE["code"])

def runCmd(cmd):
    """
    This method runs a command and returns a list
    with the contents of its stdout and stderr and
    the exit code of the command.
    """
    shell_cmd = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE, close_fds=True)
    (handleChildStdin,handleChildStdout,handleChildStderr) = (shell_cmd.stdin, shell_cmd.stdout, shell_cmd.stderr)
    childStdout = handleChildStdout.read()
    childStderr = handleChildStderr.read()
    shell_cmd.wait()
    return [childStdout, childStderr, shell_cmd.returncode]

def derivativeRecord(filePath):

    if((globalvars.destfiletype == "")):
        print("Source filetype '{}' and re-dimension value '{}' as given by in the input command."
              .format(globalvars.sourcefiletype, globalvars.resize))
    elif((globalvars.resize == "")):
        print("Source fietype '{} and destiantion filetype value '{}' as given by in the input command."
              .format(globalvars.sourcefiletype, globalvars.destfiletype))
    else:
        print("Source filetype '{}', destination filetype '{}' and re-dimension value '{}' as given by in the input command."
              .format(globalvars.sourcefiletype, globalvars.destfiletype, globalvars.resize))

    for path, subdirs, files in os.walk(filePath):
        for name in files:
            derRes = ""
            queryName = name.split(".")[0]
            derFileName = "_".join([queryName, globalvars.resize])
            derFileNameExt = ".".join([derFileName, globalvars.destfiletype])

            if derFileNameExt in files:
                print_error(errorcodes.ERROR_FILE_EXISTS["message"])
                globalvars.derivativeErrorList.append([errorcodes.ERROR_FILE_EXISTS["message"].format(derFileNameExt)])
                errorCSV()
                exit(errorcodes.ERROR_FILE_EXISTS["code"])
            else:
                records = globalvars.dbHandle[globalvars.dbCollection].find({"_id": queryName})
                records = [record for record in records]
                if(len(records) > 0):
                    for document in records:
                        if "technical" in document:
                            xRes = document['technical']['image']['xResolution']
                            yRes = document['technical']['image']['yResolution']
                            width = document['technical']['image']['width']
                            height = document['technical']['image']['length']
                            if(xRes >= yRes):
                                derRes = "x".join([globalvars.resize, yRes])
                            else:
                                derRes = "x".join([xRes, globalvars.resize])

                            fullPath = os.path.sep.join([os.path.abspath(filePath), name])
                            derivedFilePath = os.path.sep.join([os.path.abspath(filePath), derFileNameExt])
                            # execute the command "convert <original_filePath> -resize 64x64 <derived_filePath>" to generate derivative image.
                            commandInput = " ".join(['convert', fullPath, '-resize', derRes, derivedFilePath])
                            output, error, exitcode = runCmd(commandInput)

                            migration = createMigrationEvent(globalvars.destfiletype, derRes, width, height, derFileNameExt)
                            print_info("The following record has been initialized for the file: '{}': {}".format(derFileNameExt, migration))
                            document['premis']['eventList'].append(migration)
                            dbUpdatePremisProfile = updateRecordInDB(queryName, document)

if __name__ == "__main__":
    main()
