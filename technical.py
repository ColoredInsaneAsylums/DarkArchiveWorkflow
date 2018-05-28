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
# File Name: technical.py
# Description: This file contains source code for the core functionality of the
#              technical schema workflow.
#
# Creator: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#
# IMPORT NEEDED MODULES
import csv
import sys
import shutil
import re
from datetime import datetime
from time import localtime, time, strftime
from subprocess import PIPE, Popen

from metadatautilspkg.globalvars import *
from metadatautilspkg.errorcodes import *
from metadatautilspkg.technical import *
from metadatautilspkg.dbfunctions import *

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
                ver = ''
                globalvars.technicalErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver)])
                print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver))
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
            globalvars.technicalErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver)])
            print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver))
            errorCSV()
            exit(errorcodes.ERROR_INSTALL_IMAGEMAGICK["code"])
    else:
        ver = ''
        globalvars.technicalErrorList.append([errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver)])
        print_error(errorcodes.ERROR_INSTALL_IMAGEMAGICK["message"].format(ver))
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
        if firstRow == None or isFileHeaderValid(firstRow) == False:  # This also serves as a check for an empty CSV file
            print_error(errorcodes.ERROR_INVALID_HEADER_ROW["message"])
            globalvars.technicalErrorList.append([errorcodes.ERROR_INVALID_HEADER_ROW["message"]])
            errorCSV()
            exit(errorcodes.ERROR_INVALID_HEADER_ROW["code"])

        # This for loop reads and checks the format (i.errorcodes., presence of at least two
        # columns per row) of the CSV file, and populates 'globalvars.technicalList'

        rowNum = 1
        for row in csvReader:
            globalvars.technicalList.append(row)
            rowNum += 1

        csvFileHandle.close()  # Close the CSV file as it will not be needed
                            # from this point on.

    print_info("Filepath to extract technical information for: {}".format(len(globalvars.technicalList)))

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
    # print("db collection name: ".format(globalvars.dbCollection))

    # PROCESS ALL RECORDS
    for row in globalvars.technicalList:
        filePath = row[0]

        # The loop checks for the filepath read from the csv exists in the system.
        if os.path.isdir(filePath) != True:
            globalvars.technicalErrorList.append([errorcodes.ERROR_CANNOT_FIND_DIRECTORY["message"].format(filePath)])
            print_error(errorcodes.ERROR_CANNOT_FIND_DIRECTORY["message"].format(filePath))
            errorCSV()
            exit(errorcodes.ERROR_CANNOT_FIND_DIRECTORY["code"])

        # read all the tif files in the folder and store them in technicalFileInfo.
        technicalFileInfo = [file for file in os.listdir(filePath) if file.endswith(globalvars.ext)]

        print_info("Files in the path '{}' with extension '{}' are '{}'".format(filePath, globalvars.ext, technicalFileInfo))

        # function to extract technical properties of the files in technicalFileInfo.
        technicalStatus = technicalRecord(filePath, technicalFileInfo)

def errorCSV():
    # WRITE ALL ROWS THAT COULD NOT BE PROCESSED TO A CSV FILE
    if len(globalvars.technicalErrorList) > 0:  # Because at least the header row will always be there!
        errorsCSVFileName = ("technical_profile_errors_" + strftime("%Y-%m-%d_%H%M%S", localtime(time())) + ".csv")

        try:
            errorsCSVFileHandle = open(errorsCSVFileName, 'w')
        except IOError as ioErrorCsvWrite:
            print_error(ioErrorCsvWrite)
            print_error(errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["message"])
            exit (errorcodes.ERROR_CANNOT_WRITE_CSV_FILE["code"])

        csvWriter = csv.writer(errorsCSVFileHandle, delimiter=',', quotechar='"', lineterminator='\n')

        for row in globalvars.technicalErrorList:
            csvWriter.writerow(row)

        errorsCSVFileHandle.close()
        print_error("Errors were encountered and has been written to the following file: {}".format(errorsCSVFileName))

def defineCommandLineOptions():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    argParser = argparse.ArgumentParser(description="Migrate Files for Preservation")
    argParser.add_argument('-e', '--extension', nargs=1, default='*', help='Specify file EXTENSION for files that need to be migrated.') # tif files supported
    argParser.add_argument('-f', '--file', nargs=1, default=False, metavar='CSVPATH', help='CSVPATH is the path to the CSV file to be used with the -f option.')
    argParser.add_argument('-q', '--quiet', action='store_true', help='Enable this option to suppress all logging, except critical error messages.')
    # argParser.add_argument('-h', '--showhelp', action='store_true', help='Gives the argument options')
    return argParser

def parseCommandLineArgs(argParser, args):
    parsedArgs = argParser.parse_args(args)

    if len(args) == 0:
        print_error(errorcodes.ERROR_INVALID_ARGUMENT_STRING["message"])
        argParser.print_help()
        exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING["code"])

    globalvars.ext = parsedArgs.extension[0]
    globalvars.quietMode = parsedArgs.quiet
    # globalvars.help = parsedArgs.showhelp

    if parsedArgs.file:
        globalvars.batchMode = True
        globalvars.csvFile = parsedArgs.file[0]

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

def technicalRecord(filePath, technicalFileInfo):
    """technicalRecord(): Carries out the extraction of the properties of the files.

    Arguments:
        [1] filePath - path of the directory from which the properties of the files are extracted;
        [2] technicalFileInfo - list of files with the file extension same as the value in fileExt
    """

    for name in technicalFileInfo:

        fileName = os.path.splitext(name)[0]
        records = globalvars.dbHandle[globalvars.dbCollection].find({"_id": fileName})      # query the database to find the appropriate record to update technical properties.
        records = [record for record in records]                                            # store the records retrieved from the database

        if(len(records) == 1):
            
            seq = (filePath, name)
            fullPath = os.path.sep.join(seq)
            # execute the command "identify -verbose <filename>" to fetch image properties.
            output, error, exitcode = runCmd('identify -verbose ' + fullPath)

            lines = output.decode('utf-8').split('\n')              # read the output from the command
                                                                    # decode the output in the 'utf-8' format and remove line spoces.

            data = []
            for str in lines:
                if ': ' in str:
                    data.append(str)

            data = [word.replace(':  ',':') for word in data]

            # method to convert the list into dictionary to have key-value pairs.
            prop = {}
            for item in data:
                key, value = item.split(": ")
                key = key.strip(" ")
                value = value.strip(" ")
                prop[key] = value

            # read the required technical properties from the dictionary
            if 'filename' in prop:
                technicalFile = re.split(r'[/]', prop['filename'])
                technicalFileNameExt = technicalFile[-1]
                technicalFileName = technicalFileNameExt.split(".")[0]
            else:
                technicalFileNameExt = ''
                technicalFileName =  ''

            if 'Geometry' in prop:
                imageGeometry = re.split(r'[x+]', prop['Geometry'])
                imageWidth = imageGeometry[0]
                imageLength = imageGeometry[1]
            else:
                imageWidth = ''
                imageLength = ''

            if 'Depth' in prop:
                depth = prop['Depth']
                if '8' in depth:
                    bitsPerSample = 'GrayScale'
                elif '24' in depth:
                    bitsPerSample = '24-bit color'
                else:
                    bitsPerSample = ''
            else:
                bitsPerSample = ''

            if 'Compression' in prop:
                compression = prop['Compression']
                if 'None' not in compression:
                    compression = 'CCITT group 4'
            else:
                compression = ''

            if 'tiff:photometric' in prop:
                photometricInterpretation = prop['tiff:photometric']
                if 'RGB' in photometricInterpretation:
                    samplesPerPixel = '3'
                elif 'black' in photometricInterpretation:
                    samplesPerPixel = '1'
                else:
                    samplesPerPixel = '4'
                if (int(samplesPerPixel) > 3):
                    extraSamples = samplesPerPixel - 3
                    extraSamplesFlag = True
                else:
                    extraSamples = '0'
                    extraSamplesFlag = True
            else:
                photometricInterpretation = ''
                samplesPerPixel = ''
                extraSamples = ''
                extraSamplesFlag = False

            if 'Resolution' in prop:
                resolution = re.split(r'[x]', prop['Resolution'])
                xResolution = resolution[0]
                yResolution = resolution[1]
            else:
                xResolution = ''
                yResolution = ''

            if 'Units' in prop:
                resolutionUnit = prop['Units']
            else:
                resolutionUnit = ''

            if 'Colorspace' in prop:
                colorSpace = prop['Colorspace']
            else:
                colorSpace = ''

            if 'Background color' in prop:
                backgroundColor = prop['Background color']
                backgroundColorFlag = True
            else:
                backgroundColorFlag = False
                backgroundColor = ''

            if 'Border color' in prop:
                borderColor = prop['Border color']
                borderColorFlag = True
            else:
                borderColor = ''
                borderColorFlag = False

            if 'Matte color' in prop:
                matteColor = prop['Matte color']
                matteColorFlag = True
            else:
                matteColor = ''
                matteColorFlag = False

            if 'Transparent color' in prop:
                transparentColor = prop['Transparent color']
                transparentColorFlag = True
            else:
                transparentColor = ''
                transparentColorFlag = False

            if 'tiff:rows-per-strip' in prop:
                rowsPerStrip = prop['tiff:rows-per-strip']
                rowsPerStripFlag = True
            else:
                rowsPerStrip = ''
                rowsPerStripFlag = False

            if 'tiff:endian' in prop:
                endian = prop['tiff:endian']
                endianFlag = True
            else:
                endian = ''
                endianFlag = False

            if 'Orientation' in prop:
                orientation = prop['Orientation']
                orientationFlag = True
            else:
                orientation = ''
                orientationFlag = False

            # convert the time to EDTF format
            if 'tiff:timestamp' in prop:
                scanDateTime = prop['tiff:timestamp']
                date_format = datetime.strptime(scanDateTime, '%Y:%m:%d %H:%M:%S')
                new_format = date_format.strftime("%Y-%m-%d %H:%M:%S")
                timeStamp = new_format.replace(' ', 'T')
                timeZone = strftime('%z', localtime())
                timeZone = timeZone[:3] + ":" + timeZone[3:]
                scanDateTime = timeStamp + timeZone
            else:
                scanDateTime = ''

            if 'tiff:make' in prop:
                make = prop['tiff:make']
                makeFlag = True
            else:
                make = ''
                makeFlag = False

            if 'icc:model' in prop:
                model = prop['icc:model']
                modelFlag = True
            else:
                model = ''
                modelFlag = False

            if 'tiff:software' in prop:
                software = prop['tiff:software']
                softwareFlag = True
            else:
                software = ''
                softwareFlag = False

            # create a dictionary to store the missing technical property valueself. Using the flag values, the metadata is formed.
            document = {}
            document['rpsFlag'] = rowsPerStripFlag
            document['endFlag'] = endianFlag
            document['oriFlag'] = orientationFlag
            document['extFlag'] = extraSamplesFlag
            document['bacFlag'] = backgroundColorFlag
            document['borFlag'] = borderColorFlag
            document['matFlag'] = matteColorFlag
            document['traFlag'] = transparentColorFlag
            document['mkeFlag'] = makeFlag
            document['modFlag'] = modelFlag
            document['sofFlag'] = softwareFlag

            # read the technical property metadata document schema and create the metadata record to update the database.
            metadataRecord = createtechnicalProfile(document)

            if(imageWidth != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_width.name] = imageWidth
            if(imageLength != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_length.name] = imageLength
            if(bitsPerSample != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_bitsPerSample.name] = bitsPerSample
            if(compression != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_compression.name] = compression
            if(photometricInterpretation != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_photometricInterpretation.name] = photometricInterpretation
            if(samplesPerPixel != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_samplesPerPixel.name] = samplesPerPixel
            if(xResolution != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_xResolution.name] = xResolution
            if(yResolution != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_yResolution.name] = yResolution
            if(resolutionUnit != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_resolutionUnit.name] = resolutionUnit
            if(colorSpace != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_colorSpace.name] = colorSpace
            if(extraSamples != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_extraSamples.name] = extraSamples
            if(backgroundColor != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_backgroundColor.name] = backgroundColor
            if(borderColor != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_borderColor.name] = borderColor
            if(matteColor != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_matteColor.name] = matteColor
            if(transparentColor != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_transparentColor.name] = transparentColor

            if(scanDateTime != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.tech_scanDateTime.name] = scanDateTime

            if(make != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_make.name] = make
            if(model != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_model.name] = model
            if(software != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_software.name] = software

            if(rowsPerStrip != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.tech_rowsPerStrip.name] = rowsPerStrip

            if(endian != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.tech_endian.name] = endian

            if(orientation != ''):
                metadataRecord[globalvars.labels.tech_entity.name][globalvars.labels.tech_orientation.name] = orientation

            print_info("The following record has been initialized for the file: '{}': {}".format(technicalFileNameExt, metadataRecord))

            # Update the database record matching the id and add the metadata technical profile.
            dbUpdateTechProfile = updateRecordInDB(technicalFileName, metadataRecord)

        else:
            globalvars.technicalErrorList.append([errorcodes.ERROR_CANNOT_FIND_DOCUMENT["message"].format(fileName)])
            print_error(errorcodes.ERROR_CANNOT_FIND_DOCUMENT["message"].format(fileName))
            errorCSV()
            exit(errorcodes.ERROR_CANNOT_FIND_DOCUMENT["code"])

if __name__ == "__main__":
    main()
