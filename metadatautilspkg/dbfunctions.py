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
# Update: Milind Siddhanti (milindsiddhanti at utexas dot edu)
#

import json
import pymongo
import os

import metadatautilspkg.globalvars as globalvars
import metadatautilspkg.errorcodes as errorcodes
from metadatautilspkg.metadatautils import *


dbConfFileName = os.path.join(globalvars.configDir, "dbconf.json")

def init_db():
    """init_db():

    Arguments:
        none

    Reads the DB Configuration file, creates a connection to the database,
    and returns a handle to the connected database
    """

    try:
        dbConfigJson = open(dbConfFileName, "r").read()
    except IOError as exception:
        print_error(exception)
        print_error(errorcodes.ERROR_CANNOT_READ_DBCONF_FILE["message"])
        quit(errorcodes.ERROR_CANNOT_READ_DBCONF_FILE["code"])

    dbConfig = json.loads(dbConfigJson)
    dbAddr = dbConfig['dbaddress']
    dbUser = dbConfig['dbuser']
    #dbPass = urllib.quote_plus(dbConfig['dbpassword'])
    dbPass = dbConfig['dbpassword']
    dbName = dbConfig['dbname']
    globalvars.dbCollection = dbConfig['dbcollection']

    try:
        handle = pymongo.MongoClient(dbAddr)[dbName]
    except pymongo.errors.ConnectionFailure as ExceptionConnFailure:
        print_error(ExceptionConnFailure)
        print_error(errorcodes.ERROR_CANNOT_CONNECT_TO_DB["message"])
        exit(errorcodes.ERROR_CANNOT_CONNECT_TO_DB["code"])

    try:
        handle.authenticate(dbUser, dbPass)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        print_error(errorcodes.ERROR_CANNOT_AUTHENTICATE_DB_USER["message"])
        exit(errorcodes.ERROR_CANNOT_AUTHENTICATE_DB_USER["code"])

    dbParamsDict = dict()
    dbParamsDict["handle"] = handle
    dbParamsDict["collection_name"] = globalvars.dbCollection

    return dbParamsDict


def insertRecordInDB(metadataRecord):
    """insertRecordInDB

    Arguments:
        metadataRecord: the metadata record to be inserted

    This function creates a database entry pertaining to the file being transferred.

    """

    try:
        dbInsertResult = globalvars.dbHandle[globalvars.dbCollection].insert_one(metadataRecord)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        print_error(errorcodes.ERROR_CANNOT_INSERT_INTO_DB["message"])
        return(errorcodes.ERROR_CANNOT_INSERT_INTO_DB["code"])

    return(str(dbInsertResult.inserted_id))

def updateRecordInDB(id, metadataRecord):
    """updateRecordInDB

    Arguments:
        id: id of the metadata record to be updated
        metadataRecord: the metadata record to be inserted to the existing record

    This function updates a database entry pertaining to the file transferred.

    """

    try:
        dbUpdateResult = globalvars.dbHandle[globalvars.dbCollection].update_one({'_id' : id}, {'$set' : metadataRecord})
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        print_error(errorcodes.ERROR_CANNOT_UPDATE_DB["message"])
        return(errorcodes.ERROR_CANNOT_UPDATE_DB["code"])

    return(str(dbUpdateResult.upserted_id))

def deleteRecordFromDB(id):
    """deleteRecordFromDB

    Arguments:
        id: id of the metadata record to be deleted

    This function deletes a database entry corresponding to the id specified.

    """

    retVal = globalvars.dbHandle[globalvars.dbCollection].delete_one({'_id': id})

    if retVal.deleted_count != 1:
        print_error(errorcodes.ERROR_CANNOT_REMOVE_RECORD_FROM_DB["message"])
        exit(errorcodes.ERROR_CANNOT_REMOVE_RECORD_FROM_DB["code"])


def getHighestSerialNo(dirName):
    """getHighestSerialNo

    Arguments:
        dirName: the directory/path that needs to be looked up in the database.

    This function finds the highest serial number corresponding to the specified dirName.
    dirName is expected to match one of the fields called "originalName" in the PREMIS entity.

    """

    queryField = ".".join([globalvars.labels.pres_entity.name, globalvars.labels.obj_entity.name, globalvars.labels.obj_orig_name.name])
    serialNoLabel = ".".join([globalvars.labels.admn_entity.name, globalvars.labels.arrangement.name, globalvars.labels.serial_nbr.name])
    records = globalvars.dbHandle[globalvars.dbCollection].find({queryField: {"$regex": dirName}}, {"_id": 0, serialNoLabel: 1})
    records = [record for record in records]

    if len(records) == 0:
        # code changes to copy all the files from source to destination
        # return 1
        return 0
    else:
        serialNos = [int(record[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name]) for record in records]
        return max(serialNos)
