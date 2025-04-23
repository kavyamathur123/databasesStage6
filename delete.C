/*
 *
 *  By Masa Abboud (mabboud@wisc.edu), Kavya Mathur (kmathur3@wisc.edu), Marissa Pederson (mapederson4@wisc.edu)
 *
 *  This file contains the implementation of the QU_Delete function.
 *  It deletes all records from a specified relation that satisfy a given condition.
 * 
 */

#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 *  OK on success
 *  an error code otherwise
 */

 /*
 This function will delete all tuples in relation satisfying 
 the predicate specified by attrName, op, and the constant attrValue. 
 type denotes the type of the attribute. You can locate all the qualifying 
 tuples using a filtered HeapFileScan.
 */
const Status QU_Delete(const string & relation, 
               const string & attrName, 
               const Operator op,
               const Datatype type, 
               const char *attrValue)
{

    // part 6
    Status status;
    AttrDesc attrDesc;
    RID rid;

    //heap file creation
    HeapFileScan* hfs;
    hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        delete hfs;
        return status;
    }

    //same process as scan?
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
        delete hfs;
        return status;
    }

    const char* value = NULL;
    int temporaryInt;
    float temporaryFloat;

    if (type == INTEGER) {
        temporaryInt = atoi(attrValue);
        value = (char*)&temporaryInt;
        status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, INTEGER, value, op);
    }
    else if (type == FLOAT) {
        temporaryFloat = atof(attrValue);
        value = (char*)&temporaryFloat;
        status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, FLOAT, value, op);
    }
    else if (type == STRING) {
        value = attrValue;
        status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, STRING, value, op);
    }

    //deletion/memory leak prevent
    if (status != OK) {
        delete hfs;
        return status;
    }

    //actual deletion
    while ((status = hfs->scanNext(rid)) == OK) {
        status = hfs->deleteRecord();

        if (status != OK) {
            delete hfs;
            return status;
        }
    }

    if (status != FILEEOF) {
        return status;
    }

    //make sure finished reading file
    hfs->endScan();
    delete hfs;
    return OK;

}



