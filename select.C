#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*



+ Make sure to give ScanSelect the proper input
+ To go from attrInfo to attrDesc, need to consult the catalog (attrCat and relCat,
global variables)
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
//parse first 



	//SELECT attr_list [INTO relname] FROM table_list [WHERE condition]
	//performs a selection using HeapFileScan()
	//projection should be done on the fly as each result tuple is being appended to the result table
		//this means that as we process each tupe (row), that satisfies the select condition, we should immediately extract the required attributes (columns) and
		//write the partial result to output
	//string attrValue should be converted it to the proper type based on the type of attr
	//use the atoi() function to convert a char* to an integer and atof() to convert it to a float
	//if attr is NULL, an unconditional scan of the input table should be performed

   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

}

//simplified sql, SELECT, INTO, WHERE, FROM
/*

ScanSelect
+ have a temporary record for output table
+ open "result" as an InsertFileScan object
+ open current table (to be scanned) as a HeapFileScan object
+ check if an unconditional scan is required
+ check attrType: INTEGER, FLOAT, STRING
+ scan the current table
+ if find a record, then copy stuff over to the temporary record (memcpy)
+ insert into the output table
========================================

*/
const Status ScanSelect(const string & result, 
			const int projCnt, //How many columns to project (length of projNames)
			const AttrDesc projNames[],// List of attributes (columns) to include in the result
			const AttrDesc *attrDesc, //If not NULL, describes the column used in the WHERE clause
			const Operator op, //The comparison operator (like =, <, >=, etc.)
			const char *filter,//The value to compare against in the WHERE clause
			const int reclen)//	Total length of each output record
{

/*
1)Create a temporary record to hold data during processing
2)Open a "result" table where selected records will be inserted
3)Open the source table that needs to be scanned
4)Determine if all records should be selected (unconditional scan) or if there are conditions
5)Handle different data types (INTEGER, FLOAT, STRING) appropriately during the scan
For each record found in the source table:

	1)Copy the record data to the temporary record
	2)Insert this temporary record into the output/result table */

	//create a tempRecord
    char* tempData = new char[reclen];
    Record* tempRec = new Record();
    tempRec->data = tempData;
    tempRec->length = reclen;

	//create heapfileScan for opening current tabe to be scanned
	Status status;
	HeapFileScan*  hfs;
	hfs = new HeapFileScan(projNames[0].relName, status);
	if (status != OK) {
        return status;
    }

	//You are writing the resulting rows of your SELECT query 
	//into another table, and InsertFileScan is the tool (class) 
	//provided in your system to handle inserting records into a table.
	Status status2;
	InsertFileScan* ins;
	ins = new InsertFileScan(projNames[0].relName, status2);
	//if the status of insert not ok, we need to delete the heapfile?
	if (status2 != OK) {
        delete hfs;
        delete[] tempData;
        return status2;
    }
	if (attrDesc != NULL){ //check if where clause is null. if attr!= NULL, this means there is a where clause 
	// convert filter to correct type
			/*
			The filter is a raw C-style string (char*) but you need to compare it based on the actual attribute type (like int, float, or string).
			SO
			Convert it to int if the column is an INTEGER
			Convert it to float if it's a FLOAT
			Leave it as-is for STRING
			This ensures that when you apply the WHERE condition, you’re comparing the right types.
			*/
		if (attrDesc->attrType == INTEGER) {
			int value = atoi(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
								INTEGER, (char*)&value, op);
		}
		else if (attrDesc->attrType == FLOAT) {
			float value = atof(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
								FLOAT, (char*)&value, op);
		}
		else if (attrDesc->attrType == STRING) {
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, 
								STRING, filter, op);
		}
		else {
			// Unknown type what to return??
			status = BADFILE;
    		}
	} else { // if attr = Null
		//perform unconditional scan of input table (because this means there is no WHERE clause, so no filter condiiton)
		//start scan with nulld to scan everything
		
		status = hfs->startScan(0, 0, STRING, NULL, EQ);
		if (status != OK) {
			delete hfs;
			delete ins;
			delete[] tempData;
			delete tempRec;
			return status;
		}
	}

	//record handling (when attr == NULL AND attr != NULL)
		//1)Copy the record data to the temporary record
		//2)Insert this temporary record into the output/result table

		RID rid;
		Record rec;

		//for each tuple
		while ((status=hfs->scanNext(rid)) == OK) {
			//process row satisfies select condition
			status = hfs->getRecord(rec);
			if (status != OK) break;

			//extract attributes immediatly
			memset(tempData, 0, reclen);

			for (int i = 0; i < projCnt; i++) {
				//extract attributes immediately
				memcpy(tempData + projNames[i].attrOffset, 
					  (char*)rec.data + projNames[i].attrOffset,
					  projNames[i].attrLen);
			}

			RID outrid;
			//write result to output
			status = ins->insertRecord(*tempRec, outrid);
			if (status != OK) break;
		}

		    // finish and ret
			delete hfs;
			delete ins;
			delete[] tempData;
			delete tempRec;
			return (status == FILEEOF) ? OK : status;

    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
