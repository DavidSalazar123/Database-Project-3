/*
 * -----------------------------------------------------------------------------
 * File name: buf.C
 * -----------------------------------------------------------------------------
 * Created by: 
 * David Salazar - 9084824631
 * Viktor Sakman -  9083266065
 * Riza Kaya - 9081992647
 * -----------------------------------------------------------------------------
 * Purpose: 
 * This file implements the core functionalitiees of a Buffer Manager which 
 * handles the stroage of database pages in memory. The Buffer Manager uses
 * a Clock Algorithm for page replacement.
 * -----------------------------------------------------------------------------
*/

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
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

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * @brief 
 * Allocates a buffer frame using the Clock algorithm.
 * 
 * This method implements the Clock page-replacement algorithm to allocate a buffer frame. 
 * It searches for an unused frame in the buffer pool (Only loops through 2 times) 
 * and, if necessary, writes a dirty frame back to disk before replacing it. 
 * If all frames are pinned, it returns an error.
 *
 * @param frame Reference to an integer where the index of the allocated buffer 
 *              frame will be stored upon method success.
 * 
 * @return 
 * Status indicating the outcome of the operation. It returns OK if a buffer frame 
 * is successfully allocated. UNIXERR if the call to the I/O layer returned an error 
 * when a dirty page was being written to disk.
 * Returns BUFFEREXCEEDED if all frames are currently pinned and cannot be allocated.
 **/
const Status BufMgr::allocBuf(int &frame)
{
    int numTries = 0;

    // (Note: double check that it should be <= instead of <)
    while (numTries <= 2 * numBufs) // Ensure only two full rotations
    {
        advanceClock(); // Advance Clock pointer
        BufDesc &frameDesc = bufTable[clockHand];

        if (!frameDesc.valid) // If the frame is not valid, return it
        {
            frame = clockHand;
            return OK;
        }

        if (frameDesc.refbit) // If the frame is referenced, clear the refbit and advance the clock pointer
        {
            frameDesc.refbit = false;
            numTries++;
            continue;
        }

        if (frameDesc.pinCnt > 0) // If the frame is pinned, advance the clock pointer
        {
            numTries++;
            continue;
        }

        if (frameDesc.dirty) // If the frame is dirty, write it back to disk
        {
            Status s = frameDesc.file->writePage(frameDesc.pageNo, &bufPool[clockHand]);
            if (s != OK)
                return UNIXERR;
            frameDesc.dirty = false;
        }

        hashTable->remove(frameDesc.file, frameDesc.pageNo); // Book Keeping...
        frame = clockHand;
        return OK;
    }

    return BUFFEREXCEEDED; // If we get here, return BUFFEREXCEEDED (all buffer frames are pinned)
}

/**
 * @brief 
 * Reads a page from the file into the buffer pool
 *
 * @param file Pointer to File from which the page will be read
 * @param PageNo Page number to read the file from
 * @param page Reference to the page with a pointer 
 * 
 * @return  
 * Status indicating the outcome of the operation. 
 * OK if we are able to read a page from the file
 * UNIXERR if the call to the I/O layer returned an error 
 * BUFFEREXCEEDED if all frames are currently pinned
 * HASHTBLERROR if hash table error occured
 **/
const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    // Lookup the page in the hashtable and assign the frame number to frameNo
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    // Case 1: Page is not in the buffer pool.
    if (status == HASHNOTFOUND)
    {
        if ((status = allocBuf(frameNo)) != OK)
            return status; // Try to allocate a buffer frame...

        if ((status = file->readPage(PageNo, &bufPool[frameNo])) != OK)
            return status; // Try to read the page from disk into the buffer pool frame...

        if ((status = hashTable->insert(file, PageNo, frameNo)) != OK)
            return status; // Try to insert the page into the hashtable...

        bufTable[frameNo].Set(file, PageNo); // Set() on the frame to set it up properly
        page = &bufPool[frameNo];
    }

    // Case 2: Page is in the buffer pool. (Don't need to use Set())
    else if (status == OK)
    {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
    }

    return status; // Return OK
}

/**
 * @brief 
 * Decrements the pinCnt of the frame that contains the file and Page Number
 *
 * @param file Pointer to File from which the page will be read
 * @param PageNo Page number to read the file from
 * @param dirty If the tuple was altered/updated
 * 
 * @return  
 * Status indicating the outcome of the operation. 
 * OK if we are able to read a page from the file
 * HASHNOTFOUND if page is not in the buffer pool hash table
 * PAGENOTPINNED if the pin count is already 0
 **/
const Status BufMgr::unPinPage(File *file, const int PageNo, const bool dirty)
{
    // Check if page is in the buffer pool hash table and get the frame number
    int frameNo = 0;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK)
        return status; // If status is not OK (which implies HASHNOTFOUND), directly return it.

    if (bufTable[frameNo].pinCnt <= 0)
        return PAGENOTPINNED; // If pinCnt is already 0, return PAGENOTPINNED

    bufTable[frameNo].pinCnt--;
    if (dirty)                          
        bufTable[frameNo].dirty = true; // if dirty == true, sets the dirty bit

    return OK;
}

/**
 * @brief 
 * Allocates a frame into the buffer pool at the page number
 *
 * @param file Pointer to File from which the page will be read
 * @param pageNo Page number to read the file from
 * @param page The page used in the buffer pool
 * 
 * @return  
 * Status indicating the outcome of the operation. 
 * OK if we are able to read a page from the file
 * UNIXERR if a Unix error occurred
 * BUFFEREXCEEDED if all buffer frames are pinned
 * HASHTBLERROR if a hash table error occurred
 **/
const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    // Allocate a page in the file
    Status status = file->allocatePage(pageNo);
    if (status != OK)
        return status;

    // Allocate a buffer frame and set the frame number to frameNo
    int frameNo = 0;
    status = allocBuf(frameNo); // allocBuf() does the book keeping for us
    if (status != OK)
        return status;

    // Insert the page into the hashtable
    status = hashTable->insert(file, pageNo, frameNo);
    if (status != OK)
        return status;

    // Set up the frame properly and return a pointer to the frame containing the page
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    return OK; // If we get here, return OK
}

const Status BufMgr::disposePage(File *file, const int pageNo)
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

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

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
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
