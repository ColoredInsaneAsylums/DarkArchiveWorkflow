import json
import pymongo

import globalvars
import errorcodes

def init_db():
    """init_db():

    Arguments:
        none

    Reads the DB Configuration file, creates a connection to the database, 
    and returns a handle to the connected database
    """
    
    try:
        dbConfigJson = open("dbconf.json", "r").read()
    except IOError as exception:
        print_error(exception)
        print_error("\nCould not read the DB Configuration file 'dbconf.json'")
        quit(errorcodes.ERROR_CANNOT_READ_DBCONF_FILE)

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
        exit(errorcodes.ERROR_CANNOT_CONNECT_TO_DB)

    try:
        handle.authenticate(dbUser, dbPass)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        exit(errorcodes.ERROR_CANNOT_AUTHENTICATE_DB_USER)

    dbParamsDict = dict()
    dbParamsDict["handle"] = handle
    dbParamsDict["collection_name"] = globalvars.dbCollection

    return dbParamsDict


def insertRecordInDB(mdr):
    """insertRecordInDB

    Arguments:
        mdr: the metadata record to be inserted

    This function creates a database entry pertaining to the file being transferred.
    
    """

    try:
        dbInsertResult = globalvars.dbHandle[globalvars.dbCollection].insert_one(mdr)
    except pymongo.errors.PyMongoError as ExceptionPyMongoError:
        print_error(ExceptionPyMongoError)
        return(errorcodes.ERROR_CANNOT_INSERT_INTO_DB)
    
    return(str(dbInsertResult.inserted_id))


def DeleteRecordFromDB(id):
    retVal = globalvars.dbHandle[globalvars.dbCollection].delete_one({'_id': id})
    
    if retVal.deleted_count != 1:
        print_error("Cannot remove record from DB")
        exit(errorcodes.ERROR_CANNOT_REMOVE_RECORD_FROM_DB)


def getHighestSerialNo(dirName):
    queryField = ".".join([globalvars.labels.pres_entity.name, globalvars.labels.obj_entity.name, globalvars.labels.obj_orig_name.name])
    serialNoLabel = ".".join([globalvars.labels.admn_entity.name, globalvars.labels.arrangement.name, globalvars.labels.serial_nbr.name])
    records = globalvars.dbHandle[globalvars.dbCollection].find({queryField: {"$regex": dirName}}, {"_id": 0, serialNoLabel: 1})
    records = [record for record in records]

    if len(records) == 0:
        return 1
    else:
        serialNos = [int(record[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name]) for record in records]
        return max(serialNos)
