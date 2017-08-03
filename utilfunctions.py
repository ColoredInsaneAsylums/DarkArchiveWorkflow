import sys
from datetime import datetime
from time import localtime, time, strftime
import argparse
import hashlib
import json
from collections import namedtuple
from uuid import uuid4

import globalvars
import errorcodes

def defineCommandLineOptions():
    #PARSE AND VALIDATE COMMAND-LINE OPTIONS
    argParser = argparse.ArgumentParser(description="Migrate Files for Preservation")
    argParser.add_argument('-e', '--extension', nargs=1, default='*', help='Specify file EXTENSION for files that need to be migrated.')
    #argParser.add_argument('srcDstPair', nargs='*', metavar='SRC DST', help='Migrate files from SRC to DST. DST will be created if it does not exist. These arguments will be ignored if the -f option is specified.')
    argParser.add_argument('-f', '--file', nargs=1, default=False, metavar='CSVPATH', help='CSVPATH is the path to the CSV file to be used with the -f option.')
    argParser.add_argument('-q', '--quiet', action='store_true', help='Enable this option to suppress all logging, except critical error messages.')
    argParser.add_argument('-m', '--move', action='store_true', help='Enable this option to move the files instead of copying them.')

    return argParser

def parseCommandLineArgs(argParser, args):
    parsedArgs = argParser.parse_args(args)

    if len(args) == 0:
        argParser.print_help()
        exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING)

    globalvars.ext = parsedArgs.extension[0]
    globalvars.quietMode = parsedArgs.quiet
    globalvars.move = parsedArgs.move

    if parsedArgs.file:
        globalvars.batchMode = True
        globalvars.csvFile = parsedArgs.file[0]
    else:
        globalvars.batchMode = False
        if len(parsedArgs.srcDstPair) != 2:
            src = parsedArgs.srcDstPair[0]
            dst = parsedArgs.srcDstPair[1]
            globalvars.transferList.append([src, dst])
        else:
            argParser.print_help()
            exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING)


def getCurrentEDTFTimestamp():
    timeStamp = datetime.now().isoformat(sep='T').split('.')[0]
    timeZone = strftime('%z', localtime())
    timeZone = timeZone[:3] + ":" + timeZone[3:]
    return timeStamp + timeZone


def getFileChecksum(filePath):
    return hashlib.md5(open(filePath, 'rb').read()).hexdigest()


def readLabelDictionary():
    """readLabelDictionary()

    Arguments:
        None

    This function reads the JSON file containing entity labels to populate the label dictionary.
    This label dictionary will be used to assign labels to metadata items to be 
    recorded into a database for each transfer.
    """

    try:
        jsonObject = open(globalvars.labelsFileName, "r").read()
    except IOError as jsonReadException:
        print_error(jsonReadException)
        print_error("\nCould not read the labels file '{}'".format(globalvars.labelsFileName))
        quit(errorcodes.ERROR_CANNOT_READ_LABELS_FILE)

    try:
        labels = json.loads(jsonObject, object_hook= lambda d: namedtuple('Labels', d.keys())(*d.values()))
    except json.JSONDecodeError as jsonDecodeError:
        print_error(jsonDecodeError)
        print_error("The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.labelsFileName))
        exit(errorcodes.ERROR_INVALID_JSON_FILE)

    return labels


def readControlledVocabulary():
    try:
        jsonObject = open(globalvars.vocabFileName, "r").read()
    except IOError as jsonReadException:
        print_error(jsonReadException)
        print_error("\nCould not read the labels file '{}'".format(globalvars.vocabFileName))
        quit(errorcodes.ERROR_CANNOT_READ_VOCAB_FILE)

    try:
        jsonVocab = json.loads(jsonObject, object_hook= lambda d: namedtuple('Vocab', d.keys())(*d.values()))
    except json.JSONDecodeError as jsonDecodeError:
        print_error(jsonDecodeError)
        print_error("The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.vocabFileName))
        exit(errorcodes.ERROR_INVALID_JSON_FILE)

    return jsonVocab


def getUniqueID():
    return str(uuid4())


def getFileFormatName(fileName):
    extension = fileName.split('.')[-1]
    return extension.upper()


def getFileFormatVersion(fileName):
    extension = fileName.split('.')[-1]
    return ""  # TODO: This is just a STAND-IN for testing. NEEDS to be changed.


def isHeaderValid(hdr):
    if hdr[0] == globalvars.CSV_COL_1_NAME and hdr[1] == globalvars.CSV_COL_2_NAME:
        return True
    else:
        return False


def print_info(*args):
    """print_info(): Prints informational messages on stdout

    Arguments:
        [1] Variable length list of arguments
    
    If quiet mode is enabled, this function does nothing. If quiet mode is 
    not enabled (default) then the arguments are passed one by one to the 
    built-in print() function.
    """
    if globalvars.quietMode == False:
        print(getCurrentEDTFTimestamp() + ": ", end='')
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
