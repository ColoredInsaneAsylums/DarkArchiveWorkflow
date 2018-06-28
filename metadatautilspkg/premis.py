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
# Creator: Nitin Verma (nitin dot verma at utexas dot edu)
# 

import metadatautilspkg.globalvars as globalvars
from metadatautilspkg.metadatautils import *
from metadatautilspkg.adminmetadatautils import *

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
    """initMetadataRecord

    Arguments:
        initParams: initial parameters needed to populate the various fields
                    of the new metadata record. Passed as a dictionary object.

    This function creates a 'skeletal' metadata record, with as many fields 
    populated as possible at this initialization stage.

    """

    metadataRecord = {}
    uniqueId = getUniqueID()
    metadataRecord["_id"] = uniqueId

    # Create the ADMIN entity here:
    arrangementInfo = initParams[globalvars.ARRANGEMENT_INFO_LABEL]
    adminEntity = initAdminMetadataEntity(arrangementInfo)

    metadataRecord[globalvars.labels.admn_entity.name] = adminEntity

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

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord

def createMetadataExtractionEvent(extdMethd, extdmeta):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.metadataExt
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_mthd.name] = extdMethd
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_metadataExtraction.name] = extdmeta
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord

def createMetadataModificationEvent(before, after):
    eventRecord = {}
    eventRecord[globalvars.labels.evt_entity.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_typ.name] = globalvars.EVT_ID_TYP
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_id.name][globalvars.labels.evt_id_val.name] = getUniqueID()
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_typ.name] = globalvars.vocab.evtTyp.metadataMod
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_dttime.name] = getCurrentEDTFTimestamp()

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name] = []
    eventDetailRecord = {}  # Create a single record for event detail information
    eventDetailRecord[globalvars.labels.evt_detail_info.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name] = {}
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_before.name] = before
    eventDetailRecord[globalvars.labels.evt_detail_info.name][globalvars.labels.evt_detail_ext.name][globalvars.labels.evt_detail_after.name] = after
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_detail_parent.name].append(eventDetailRecord)

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_outcm_info.name][globalvars.labels.evt_outcm.name] = globalvars.vocab.evtOutcm.success

    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name] = {}
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_typ.name] = globalvars.LNK_AGNT_ID_TYPE
    eventRecord[globalvars.labels.evt_entity.name][globalvars.labels.evt_lnk_agnt_id.name][globalvars.labels.evt_lnk_agnt_id_val.name] = globalvars.LNK_AGNT_ID_VAL

    return eventRecord
