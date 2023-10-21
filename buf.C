#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"


#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------
BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{

}

// NOTE: Double check the if statments. Might return something other than HASHNOTFOUND or OK, then what should we do?
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    //frameNo is set correctly if lookup was succesful
    int frameNo = 0;

    // 1: First check if page is in the buffer pool by calling the lookup function to get frame number
    Status status = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Page is not in the buffer pool.  
    if (status == HASHNOTFOUND)
    {
        //Allocate a buffer frame
        status = allocBuf(frameNo);
        if (status != OK) // If allocBuf() fails, return the error status (Can be either HASHTBLERROR or BUFFEREXCEEDED or etc.)
        {
            return status; // If status is not OK, return the status
        }

        //Read the page from disk into the buffer pool frame
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK)
        {
            return status;
        }

        //Insert the page into the hashtable
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK)
        {
            return status;
        }

        //Set() on the frame to set it up properly
        bufTable[frameNo].Set(file, PageNo); // Sets pinCnt to 1
        page = &bufPool[frameNo]; // Return a pointer to the frame containing the page via the page parameter
        return status; // Return OK
    }

    //Case 2) Page is in the buffer pool.  
    else if (status == OK)
    {
        //Set the appropriate refbit
        bufTable[frameNo].refbit = true;

        //Increment the pinCnt for page
        bufTable[frameNo].pinCnt++;

        //Return a pointer to the frame containing the page via the page parameter
        page = &bufPool[frameNo];
        
        return status;
    }
    // Case 3) Some other error occurred
    else
    {
        return status;
    }
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{







}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


