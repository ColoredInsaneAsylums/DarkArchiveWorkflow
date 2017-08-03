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
        print_error(errorcodes.ERROR_INVALID_ARGUMENT_STRING["message"])
        argParser.print_help()
        exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING["code"])

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
            print_error(errorcodes.ERROR_INVALID_ARGUMENT_STRING["message"])
            argParser.print_help()
            exit(errorcodes.ERROR_INVALID_ARGUMENT_STRING["code"])


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
        print_error(errorcodes.ERROR_CANNOT_READ_LABELS_FILE["message"])
        quit(errorcodes.ERROR_CANNOT_READ_LABELS_FILE["code"])

    try:
        labels = json.loads(jsonObject, object_hook= lambda d: namedtuple('Labels', d.keys())(*d.values()))
    except json.JSONDecodeError as jsonDecodeError:
        print_error(jsonDecodeError)
        print_error(errorcodes.ERROR_INVALID_JSON_IN_LABELS_FILE["message"])
        exit(errorcodes.ERROR_INVALID_JSON_IN_LABELS_FILE["code"])

    return labels


def readControlledVocabulary():
    try:
        jsonObject = open(globalvars.vocabFileName, "r").read()
    except IOError as jsonReadException:
        print_error(jsonReadException)
        print_error(errorcodes.ERROR_CANNOT_READ_VOCAB_FILE["message"])
        quit(errorcodes.ERROR_CANNOT_READ_VOCAB_FILE["code"])

    try:
        jsonVocab = json.loads(jsonObject, object_hook= lambda d: namedtuple('Vocab', d.keys())(*d.values()))
    except json.JSONDecodeError as jsonDecodeError:
        print_error(jsonDecodeError)
        print_error(errorcodes.ERROR_INVALID_JSON_IN_VOCAB_FILE["message"])
        exit(errorcodes.ERROR_INVALID_JSON_IN_VOCAB_FILE["code"])

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


# FUNCTION DEFINITIONS 

def getEventDetails():
    return ""  # TODO: temporary, needs more work!!


def getEventOutcome():
    return ""  # TODO: temporary, needs more work!!


def getEventOutcomeDetail():
    return ""  # TODO: temporary, needs more work!!


def getLinkingAgentId():
    return ""  # TODO: temporary, needs more work!!


def getLinkingAgentRole():
    return ""  # TODO: temporary, needs more work!!


def initMetadataRecord(initParams):
    metadataRecord = {}
    uniqueId = getUniqueID()
    metadataRecord["_id"] = uniqueId

    # Create the ADMIN entity here:
    metadataRecord[globalvars.labels.admn_entity.name] = {}
    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name] = {}

    # Remove empty fields from the Arrangement dictionary
    arrangementFields = []
    for key, value in iter(initParams[globalvars.ARRANGEMENT_INFO_LABEL].items()):
        if value == "":
            arrangementFields.append(key)

    for key in arrangementFields:
        initParams[globalvars.ARRANGEMENT_INFO_LABEL].pop(key)

    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name].update(initParams[globalvars.ARRANGEMENT_INFO_LABEL])
    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name] = globalvars.MD_INIT_STRING

    # Create the PREMIS (or preservation) entity here:
    metadataRecord[globalvars.labels.pres_entity.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_id.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_id.name][globalvars.labels.obj_id_typ.name] = globalvars.OBJ_ID_TYPE
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_id.name][globalvars.labels.obj_id_val.name] = uniqueId
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_cat.name] = globalvars.vocab.objCat
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fixity.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fixity.name][globalvars.labels.obj_msgdgst_algo.name] = globalvars.MD_INIT_STRING
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fixity.name][globalvars.labels.obj_msgdgst.name] = globalvars.MD_INIT_STRING
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_size.name] = initParams["fileSize"]
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fmt.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fmt.name][globalvars.labels.obj_fmt_dsgn.name] = {}
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fmt.name][globalvars.labels.obj_fmt_dsgn.name][globalvars.labels.obj_fmt_name.name] = initParams["fmtName"]
    #metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_chars.name][globalvars.labels.obj_fmt.name][globalvars.labels.obj_fmt_dsgn.name][globalvars.labels.obj_fmt_ver.name] = initParams["fmtVer"]

    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_orig_name.name] = initParams["fileName"]
    
    # Create a parent entity (list) of all PREMIS 'event' entities.
    metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.evt_parent_entity.name] = []
    print_info("The following record has been initialized: {}".format(metadataRecord))

    return metadataRecord


def createIDAssignmentEvent(uniqueId):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.idAssgn
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    # Create a parent entity (list) for all PREMIS 'eventDetailInformation' entities
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_algo.name] = globalvars.UNIQUE_ID_ALGO
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_proglang.name] = globalvars.PYTHON_VER_STR
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_mthd.name] = globalvars.UNIQUE_ID_METHOD
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_idAssgn.name] = uniqueId

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord

def createMsgDigestCalcEvent(chksm, chksmAlgo):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.msgDgstCalc
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_algo.name] = globalvars.CHECKSUM_ALGO
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_proglang.name] = globalvars.PYTHON_VER_STR
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_mthd.name] = globalvars.CHECKSUM_METHOD
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_msgDgst.name] = chksm
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord

def createFileCopyEvent(evtTyp, srcFilePath, dstFilePath):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.replication
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_src.name] = srcFilePath
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_dst.name] = dstFilePath
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success
    #eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm_detail.name] = {}
    #eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm_detail.name][globalvars.labels.evt_outcm_detail_note.name] = "Original file successfully replicated"

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord


def createFilenameChangeEvent(dstFilePrelimPath, dstFileUniquePath):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.filenameChg
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_src.name] = dstFilePrelimPath
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_dst.name] = dstFileUniquePath
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord


def createFixityCheckEvent(status, calcChecksum):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.fixityChk
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_calc_msgDgst.name] = calcChecksum
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord


def createAccessionEvent():
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.accession
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success
    #eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm_detail.name] = {}
    #objectIdVal = metadataRecord[globalvars.labels.pres_entity.name][globalvars.labels.obj_entity.name][globalvars.labels.obj_id.name][globalvars.labels.obj_id_val.name]
    #eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm_detail.name][globalvars.labels.evt_outcm_detail_note.name] = "Object with ID '{}' successfully included in the database".format(objectIdVal)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord

def updateSerialNumber(metadataRecord, serialNbr):
    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name] = serialNbr
    return metadataRecord



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
