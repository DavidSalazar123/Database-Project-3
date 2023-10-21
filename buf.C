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

/*
Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back to disk. 
Returns BUFFEREXCEEDED if all buffer frames are pinned, 
UNIXERR if the call to the I/O layer returned an error when a dirty page was being written to disk and OK otherwise.  
This private method will get called by the readPage() and allocPage() methods described below.

Make sure that if the buffer frame allocated has a valid page in it, that you remove the appropriate entry from the hash table.
*/
const Status BufMgr::allocBuf(int & frame) 
{
    // If all buffer frames are pinned, return BUFFEREXCEEDED
    bool allPinned = true;
    for (int i = 0; i < numBufs; i++)
    {
        if (bufTable[i].pinCnt == 0)
        {
            allPinned = false;
            break;
        }
    }

    // ALl the buffer frames are pinned (pinCnt > 0)
    if (allPinned == true)
    {
        return BUFFEREXCEEDED;
    }
    
    // Because of the advanceClock() function, we will always find a victim frame
    // This means that we will always reset to the front of the "clock" with the hand
    while (true)
    {
        File* file = bufTable[clockHand].file;
        // If valid bit = false, invoke Set() on the frame to set it up properly
        if (bufTable[clockHand].valid == false)
        {
            int pageNo = bufTable[clockHand].pageNo;
            bufTable[clockHand].Set(file, pageNo);
            frame = clockHand;
            return OK;
        }

        // refbit == true and valid == true, so advance the clock
        else if(bufTable[clockHand].valid == true && bufTable[clockHand].refbit == true) 
        {
            bufTable[clockHand].refbit = false;
            advanceClock();
        }
        else if (bufTable[clockHand].pinCnt > 0)
        {
            advanceClock();
        }
        else if (bufTable[clockHand].dirty == true)
        {
            // Flush the page to disk
            Status status = flushFile(file);

            // Status status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
            if (status != OK)
            {
                return UNIXERR;
            }

            // Remove the page
            int pageNo = bufTable[clockHand].pageNo;
            bufTable[clockHand].Set(file, pageNo);
            frame = clockHand;
            return OK;
        }
        else
        {
            int pageNo = bufTable[clockHand].pageNo;
            bufTable[clockHand].Set(file, pageNo);
            frame = clockHand;
            return OK;
        }    
    }
}

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

const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
    //frameNo is set correctly if lookup was succesful
    int frameNo = 0;

    // 1: First check if page is in the buffer pool by calling the lookup function to get frame number
    Status status = hashTable->lookup(file, PageNo, frameNo);

    //Case 2) Page is in the buffer pool.  
    if (status == OK)
    {
        //Verify that the pinCnt > 0 to confirm we can decrease
        if (bufTable[frameNo].pinCnt <= 0) // Note: <= 0 might be safer to do, instead of == 0
        {
            return PAGENOTPINNED;
        }

        //Actually decrease the pinCnt
        bufTable[frameNo].pinCnt--;

        //If dirty == true, set the dirty bit
        if (dirty == true)
        {
            bufTable[frameNo].dirty = true;
        }

        //Return OK
        return status;
    }
    // Case 2) Some other error occurred (HASHNOTFOUND, UNIXERR, HASHTBLERROR)
    else
    {
        return status;
    }
}
 
// The method returns both the page number of the newly allocated page to the caller via the pageNo parameter and a pointer to the buffer frame allocated for the page via the page parameter. 
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    //Allocate a page in the file
    Status status = file->allocatePage(pageNo);

    // We could not allocate a page since the file is full
    if (status != OK)
    {
        return status;
    }

    //Allocate a buffer frame
    int frameNo = 0;
    status = allocBuf(frameNo);
    if (status != OK) // If allocBuf() fails, return the error status (Can be either HASHTBLERROR or BUFFEREXCEEDED or etc.)
    {
        return status; // If status is not OK, return the status
    }

    //Insert the page into the hashtable
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
    {
        return status;
    }

    //Set() on the frame to set it up properly
    bufTable[frameNo].Set(file, pageNo); // Sets pinCnt to 1
    page = &bufPool[frameNo]; // Return a pointer to the frame containing the page via the page parameter
    return status; // Return OK
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


