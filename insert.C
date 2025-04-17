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
status = relCat->getInfo(relation, relDesc);
Status status;
if(status != OK)
{
	return status;
}

//if the relation DOES exist, we want to create a new record with the info from attrlist
Record record;
record.length = relDesc.recordLength;
record.data = new char[record.length];
if (!record.data) {
    return INSUFMEM;
}

//mow what we have record data initiliaze ir to 0 before we can apply attricutes
memset(record.data, 0, record.length);

//set the attricbutes form attrList
for(int i = 0; i< attrCnt; i++)
{
	AttrDesc attribute;
	status = attrCat->getInfo(relation, attrList[i].attrName, attribute);

	if (status != OK)
	{
		return status;
	}

	//attrubute holds attribute, but we need to know where it goes
	    char* destination = (char*)record.data + attribute.attrOffset;

}


return OK;
}

