import globalvars

def initAdminMetadataEntity(arrangementInfo):
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
    metadataRecord[globalvars.labels.admn_entity.name][globalvars.labels.arrangement.name][globalvars.labels.serial_nbr.name] = serialNbr
    return metadataRecord
