#include <iostream>
#include <pthread.h>

class MutexLock
{
public:
    MutexLock(pthread_mutex_t* pMutex):mtx(pMutex){
        if((mtx != nullptr) && (pthread_mutex_lock(mtx) != 0))
            std::cout << "Mutex Lock Failed" << std::endl;
    }

    ~MutexLock(){
        if((mtx != nullptr) && (pthread_mutex_unlock(mtx) != 0))
            std::cout << "Mutex Unlock Failed" << std::endl;
    }

private:
    pthread_mutex_t* mtx;
};

class Buffer
{
public:
    typedef unsigned char UCHAR;
    typedef unsigned int UINT;

    void init(UCHAR* buff, UINT isize, UINT len){
        buffer = buff;
        size = isize;
        usedSize = len;
        resize(isize);
    }

    virtual bool resize(UINT isize, bool copyOldData = false){
        if(isize <= size)
            return true;
        UCHAR* newBuffer = new UCHAR [isize];
        
        if(copyOldData){
            memcpy(newBuffer, buffer, sizeof(UCHAR)*(usedSize));
        }

        clearMemory();
        buffer = newBuffer;
        size = isize;
        return true;
    }

    bool validateLength(UINT len){
        return (len <= size);
    }

    bool setLength(UINT len){
        if(!validateLength(len))
            return false;
        usedSize = len;
        return true;
    }

    UINT getUsedLength() const{
        return usedSize;
    }

    UINT getSize() const{
        return size;
    }

    UCHAR* getBuffer() const{
        return buffer;
    }

    virtual ~Buffer(){
        clearMemory();
    }

    void markShutdown(bool val){
        shutdownMsg = val;
    }

    bool isShutdownMsg(){
        return shutdownMsg;
    }

protected:
    Buffer():buffer(nullptr),size(0),usedSize(0),shutdownMsg(false) {}
    
    Buffer(UINT isize):buffer(nullptr),size(0),usedSize(0),shutdownMsg(false) {
        resize(isize);
    }

private:
    void clearMemory(){
        if(buffer == nullptr)
            return;
        delete[] buffer;
        buffer = nullptr;
        size = 0;
    }

    UCHAR* buffer;
    UINT size;
    UINT usedSize;
    bool shutdownMsg;
};

class BufferPool;
class PooledBuffer;
struct BufferQueueItem{
    PooledBuffer *pBuffer;
    BufferQueueItem *pNext;
    BufferQueueItem(PooledBuffer *buff) : pBuffer(buff), pNext(nullptr) {}
};

class PooledBuffer : public Buffer
{
public:
    PooledBuffer(BufferPool *ipool): Buffer(), pool(ipool), qItem(nullptr){
        init(nullptr, 0, 0);
    }

    virtual ~PooledBuffer() {}

    void returnToPool();

    void setPoolQueueItem (BufferQueueItem *pItem){
        qItem = pItem;
    }

    BufferQueueItem* getPoolQueueItem (){
        return qItem;
    }

private:
    BufferPool *pool;
    BufferQueueItem *qItem;
};

class BufferQueue
{
public:
    BufferQueue():head(nullptr), tail(nullptr) {
        if (pthread_mutex_init(&qLock, nullptr) != 0)
            std::cout << "Failed to Initialize Mutex" << std::endl;
        if (pthread_cond_init(&qHasData, nullptr) != 0)
            std::cout << "Failed to Initialize Conditional Variable" << std::endl;
    }

    ~BufferQueue(){
        if (pthread_cond_destroy(&qHasData) != 0)
            std::cout << "Failed to Destory Conditional Variable" << std::endl;
        if (pthread_mutex_destroy(&qLock) != 0)
            std::cout << "Failed to Destroy Mutex" << std::endl;
    }

    void addToTail(BufferQueueItem* item){
        MutexLock lock(&qLock);
        if (head == nullptr){
            head = tail = item;
            return;
        }
        tail->pNext = item;
        tail = item;
        pthread_cond_signal(&qHasData);
    }

    BufferQueueItem* removeFromHead(){
        MutexLock lock(&qLock);
        if (head == nullptr){
//            std::cout << "Waiting for Queue Items" << std::endl; 
            pthread_cond_wait(&qHasData, &qLock);
        }
        BufferQueueItem* pItem = head;
        head = head->pNext;
        pItem->pNext = nullptr;
        if (head == nullptr)
            tail = nullptr;
        return pItem;
    }

    bool isEmpty(){
        return (head == nullptr);
    }

    BufferQueueItem* getHead(){
        return head;
    }

    void clear(){
        head = tail = nullptr;
    }

private:
    BufferQueueItem* head;
    BufferQueueItem* tail;
    pthread_mutex_t qLock;
    pthread_cond_t qHasData;
};

class BufferPool
{
public:
    BufferPool():BufferSize(0),numberOfBuffers(0) {
        if (pthread_mutex_init(&m, nullptr) != 0)
            std::cout << "Failed to Initialize Pool Mutex" << std::endl;
    }

    ~BufferPool(){
        cleanup(buffers);
    }

    void init(Buffer::UINT iBufferSize, int maxBuffers){
        BufferSize = iBufferSize;
        numberOfBuffers = maxBuffers;
        for(int i=0; i<maxBuffers; i++){
            PooledBuffer *pBuffer = new PooledBuffer(this);
            pBuffer->resize(BufferSize);
//            std::cout << "Allocated buffer of size : " << pBuffer->getSize() << std::endl;
            BufferQueueItem *pItem = new BufferQueueItem(pBuffer);
            pBuffer->setPoolQueueItem(pItem);
            buffers.addToTail(pItem);
        }
    }

    BufferQueueItem* getHead(){
        return buffers.getHead();
    }

    BufferQueueItem* getFromPool(){
        BufferQueueItem* item = buffers.removeFromHead();
        MutexLock lock(&m);
        numberOfBuffers--;
        return item;//buffers.removeFromHead();
    }

    void returnToPool(BufferQueueItem *pItem){
        buffers.addToTail(pItem);
        MutexLock lock(&m);
        numberOfBuffers++;
    }

    int getPoolSize(){
        MutexLock lock(&m);
        return numberOfBuffers;
    }

private:
    void cleanup(BufferQueue& buffer){
        while(numberOfBuffers){
            BufferQueueItem* pItem = buffer.removeFromHead();
//            std::cout << "Deallocated buffer of size : " << pItem->pBuffer->getSize() << std::endl;
            if (pItem == 0) 
                break;
            if (pItem->pBuffer != 0) 
                delete pItem->pBuffer;
            delete pItem;
            numberOfBuffers--;
        }
        return;
    }

    Buffer::UINT BufferSize;
    Buffer::UINT numberOfBuffers;
    BufferQueue buffers;
    pthread_mutex_t m;
};

void PooledBuffer::returnToPool(){
    pool->returnToPool(qItem);
}

