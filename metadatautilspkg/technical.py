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
# Creator: Milind Siddhanti  (milindsiddhanti at utexas dot edu)
#

import sys
from collections import namedtuple
import argparse
import json

from metadatautilspkg.globalvars import *
from metadatautilspkg.errorcodes import *
from metadatautilspkg.metadatautils import *
from metadatautilspkg.adminmetadatautils import *

def readLabelDictionary():
    """readLabelDictionary()

    Arguments:
        None

    This function reads the JSON file containing entity labels to populate the label dictionary.
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

def isFileHeaderValid(firstrow):
    if len(firstrow) != 0:
        if firstrow[0] == globalvars.CSV_TECH_COL_1_NAME:
            return True
        else:
            return False
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

    print()

def createtechnicalProfile(document):
    """createtechnicalProfile

        Arguments:
            [1] document - dictionart that contains information of missing document values based on which the metadata is formed

        Returns:
            technicalProfile metadata record

        This function creates an empty shell of the technical profile

    """
    technicalProfile = {}

    technicalProfile[globalvars.labels.tech_entity.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_width.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_length.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_bitsPerSample.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_compression.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_photometricInterpretation.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_samplesPerPixel.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_xResolution.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_yResolution.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_resolutionUnit.name] = {}
    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_colorSpace.name] = {}
    if(document['extFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_extraSamples.name] = {}
    if(document['bacFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_backgroundColor.name] = {}
    if(document['borFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_borderColor.name] = {}
    if(document['matFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_matteColor.name] = {}
    if(document['traFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.img_entity.name][globalvars.labels.img_transparentColor.name] = {}

    technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.tech_scanDateTime.name] = {}

    if((document['mkeFlag'] == True) or (document['modFlag'] == True) or (document['sofFlag'] == True)):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name] = {}
        if(document['mkeFlag'] == True):
            technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_make.name] = {}
        if(document['modFlag'] == True):
            technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_model.name] = {}
        if(document['sofFlag'] == True):
            technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.scan_entity.name][globalvars.labels.scan_software.name] = {}

    if(document['rpsFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.tech_rowsPerStrip.name] = {}

    if(document['endFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.tech_endian.name] = {}

    if(document['oriFlag'] == True):
        technicalProfile[globalvars.labels.tech_entity.name][globalvars.labels.tech_orientation.name] = {}

    return technicalProfile
