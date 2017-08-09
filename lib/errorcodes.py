import globalvars

# ERROR CODES
ERROR_INVALID_ARGUMENT_STRING = {"code": "e01", "message": "Invalid (number of) arguments specified on the command-line."}
ERROR_CANNOT_OPEN_CSV_FILE = {"code": "e02", "message": "Cannot open the CSV batch file '{}'.".format(globalvars.csvFile)}
ERROR_CANNOT_WRITE_CSV_FILE = {"code": "e03", "message": "Could not write CSV file for errors encountered during transfers."}
ERROR_CANNOT_READ_DBCONF_FILE = {"code": "e04", "message": "Cannot read the DB configuration file."}
ERROR_INVALID_HEADER_ROW = {"code": "e05", "message": "The header in the input CSV file is invalid."}
ERROR_CANNOT_CONNECT_TO_DB = {"code": "e06", "message": "Cannot connect to the DB."}
ERROR_CANNOT_AUTHENTICATE_DB_USER = {"code": "e07", "message": "Cannot authenticate DB user specified."}
ERROR_CANNOT_INSERT_INTO_DB = {"code": "e08", "message": "Cannot insert record into the DB."}
ERROR_CANNOT_REMOVE_FILE = {"code": "e09", "message": "Cannot remove file from directory."}
ERROR_CANNOT_REMOVE_RECORD_FROM_DB = {"code": "e10", "message": "Cannot remove record from DB."}
ERROR_CANNOT_READ_LABELS_FILE = {"code": "e11", "message": "Cannot read the labels file '{}'.".format(globalvars.labelsFileName)}
ERROR_INVALID_JSON_IN_LABELS_FILE = {"code": "e12", "message": "The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.labelsFileName)}
ERROR_CANNOT_READ_VOCAB_FILE = {"code": "e13", "message": "Cannot read the vocab file '{}'.".format(globalvars.vocabFileName)}
ERROR_INVALID_JSON_IN_VOCAB_FILE = {"code": "e14", "message": "The file '{}' is not a valid JSON file. Please check the file for formatting errors.".format(globalvars.vocabFileName)}
ERROR_CANNOT_CREATE_DESTINATION_DIRECTORY = {"code": "e15", "message": "Cannot create destination directory '{}'. Skipping to next transfer."}

