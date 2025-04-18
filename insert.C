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



*/
RelDesc relDesc;
//check to see if a relation exists
Status status;
status = relCat->getInfo(relation, relDesc);
if(status != OK)
{
	return status;
}

//if the relation DOES exist, we want to create a new record with the info from attrlist
Record record;
RID rid;

record.length = sizeof(relDesc);
record.data = new char[record.length];
if (!record.data) {
    return INSUFMEM;
}

//now what we have record data initiliaze it to 0 before we can apply attributes
memset(record.data, 0, record.length);

//set the attributes form attrList

/*
typedef struct {
    char relName[MAXNAME];   // Name of the relation (e.g., "students")
    char attrName[MAXNAME];  // Name of the attribute (e.g., "gpa")
    int attrOffset;          // Offset in bytes within a record
    int attrLen;             // Length of the attribute in bytes
    Datatype attrType;       // Type (e.g., STRING, INTEGER, FLOAT)
    int indexed;             // 1 if there's an index on it, 0 otherwise
} AttrDesc;

*/


for(int i = 0; i< attrCnt; i++)
{
	//attribute holds attrList data now
	AttrDesc attribute;
	status = attrCat->getInfo(attrList[i].relName, attrList[i].attrName, attribute);

	if (status != OK)
	{
		return status;
	}

	char* destination = (char*)record.data + attribute.attrOffset;

	//now need to copy the attribute to our newly made record
	memcpy(destination, attrList[i].attrValue, attribute.attrLen);
}

	//now our record is updated, so we just need to insert i
	InsertFileScan*  ifs;
	ifs = new InsertFileScan(RELCATNAME, status);

	status = ifs->insertRecord(record, rid);

	return OK;
}

