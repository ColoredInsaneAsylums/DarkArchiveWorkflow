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

import globalvars


def initAdminMetadataEntity(arrangementInfo):
    """initAdminMetadataEntity(): Creates an administrative metadata entity.
    
    Arguments: 
        [1] arrangementInfo: information about the "arrangment" sub-entity within
                             the admin entity. Passed to this function as a
                             dictionary.
    
    Returns:
        A dictionary object containing the components of the admin entity.
    """

    adminEntity = {}
    arrangementEntity = {}

    # Remove empty fields from the arrangementInfo dictionary
    arrangementFields = []
    for key, value in iter(arrangementInfo.items()):
        if value == "":
            arrangementFields.append(key)

    for key in arrangementFields:
        arrangementInfo.pop(key)

    arrangementEntity.update(arrangementInfo)
    arrangementEntity[globalvars.labels.serial_nbr.name] = globalvars.MD_INIT_STRING

    adminEntity[globalvars.labels.arrangement.name] = arrangementEntity

    return adminEntity


def updateSerialNumber(metadataRecord, serialNbr):
    """updateSerialNumber(): Update the serial number in the arrangement info entity

    Arguments:
        [1] metadataRecord: the metadata record (dictionary object) in which the update
                            needs to be made.
        [2] serialNbr: the serial number to be recorded in the metadata record.

    Returns:
        Returns the updated metadata record (dictionary object)
    """
    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name] = serialNbr
    return metadataRecord
