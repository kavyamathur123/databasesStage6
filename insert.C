#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(
	const string & relation, //The name of the relation (table) to insert into
	const int attrCnt, //Number of attributes (columns) in the record
	const attrInfo attrList[]) //Array of attribute information 
{
    /*
    Insert a tuple with the given attribute values (in attrList) in relation. The
    value of the attribute is supplied in the attrValue member of the attrInfo
    structure. Since the order of the attributes in attrList[] may not be the same as
    in the relation, you might have to rearrange them before insertion. If no value is
    specified for an attribute, you should reject the insertion as Minirel does not
    implement NULLs.
    */
    
    RelDesc relDesc;
    //check to see if a relation exists
    Status status;
    status = relCat->getInfo(relation, relDesc);
    if(status != OK)
    {
        return status;
    }
    
    // Get all attributes for this relation to determine record size
    int attrCount;
    AttrDesc *attrs;
    status = attrCat->getRelInfo(relation, attrCount, attrs);
    if(status != OK)
    {
        return status;
    }
    
    // check that we are given all attributes
    bool allAttributesSpecified = true;
    for(int i = 0; i < attrCount; i++)
    {
        bool attributeFound = false;
        for(int j = 0; j < attrCnt; j++)
        {
            if(strcmp(attrs[i].attrName, attrList[j].attrName) == 0)
            {
                attributeFound = true;
                break;
            }
        }
        if(!attributeFound)
        {
            allAttributesSpecified = false;
            break;
        }
    }
    
    if(!allAttributesSpecified)
    {
        delete[] attrs;
        return ATTRTYPEMISMATCH; 
    }
    
    //Create a new record 
    Record record;
	record.length = sizeof(relDesc);
    record.data = new char[record.length];
    if (!record.data) {
        delete[] attrs;
        return INSUFMEM;
    }
    
	//set to 0
    memset(record.data, 0, record.length);
    
    //Set the attributes from attrList
    for(int i = 0; i < attrCnt; i++)
    {
        AttrDesc attribute;
        status = attrCat->getInfo(relation, attrList[i].attrName, attribute);
        
        if (status != OK)
        {
            delete[] record.data;
            delete[] attrs;
            return status;
        }
        
		//gets the right location to put it at bc of the attrOffset
		char* destination = (char*)record.data + attribute.attrOffset;        
        //Copy the attribute to our record
        memcpy(destination, attrList[i].attrValue, attribute.attrLen);
    }
    
    //Insert the record
    RID rid;
    InsertFileScan* ifs;
    ifs = new InsertFileScan(relation, status);
    
    if(status != OK)
    {
		delete[] (char*)record.data;
        delete[] attrs;
        delete ifs;
        return status;
    }
    
    status = ifs->insertRecord(record, rid);
    
    delete[] record.data;
    delete[] attrs;
    delete ifs;
    
    return status;
}

