#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <unordered_map>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>
#include <ctime>
#include <thread>
#include <chrono>
#include "buffers.h"

std::list<BufferQueueItem*> q;
std::vector<std::thread*> threads;
pthread_mutex_t m;
pthread_mutex_t v;
pthread_cond_t cv;
BufferPool pool;

static const int TARGET_YEAR = 2016;
static const int TOP_USERS = 10;
static const int BUFFER_SIZE = 1*1024*1024;
static const int GUARD_BUFF_SIZE = 32*1024;
static const int BUFF_READ_SIZE = BUFFER_SIZE - GUARD_BUFF_SIZE;
static const int MAX_BUFFERS = 16;
static const int USER_THREAD_COUNT = 1;
static const int THREAD_COUNT = 3;

std::string parseFieldFromLine(const std::string &line, const std::string &key) {

    std::string keyPattern = key + "=\"";
    ssize_t idx = line.find(keyPattern);

    if (idx == -1) {
        return "";
    }

    size_t start = idx + keyPattern.size();

    size_t end = start;
    while (line[end] != '"') {
        end++;
    }

    return line.substr(start, end-start);
}

int parseIntFromLine(const std::string &line, const std::string &key) {
    std::string strField = parseFieldFromLine(line, key);
    return atoi(strField.c_str());
}

struct User {
    std::string id;
    std::string displayName;
    User(std::string id, std::string displayName) : id(id), displayName(displayName) {}
};

struct Post {
    enum PostType {
        QUESTION,
        ANSWER,
        OTHER
    };

    std::string id;
    PostType postTypeId;
    std::string ownerUserId;
    std::tm creationDate;
    int score;

    Post(std::string id, std::string postTypeIdStr, std::string ownerUserId, struct std::tm creationDate, int score)
    : id(id), ownerUserId(ownerUserId), creationDate(creationDate), score(score) {
        if (postTypeIdStr == "1") {
            postTypeId = QUESTION;
        } else if (postTypeIdStr == "2") {
            postTypeId = ANSWER;
        } else {
            postTypeId = OTHER;
        }
    }
};

void pushPostData(std::vector<Post> &posts, Post &p){
    MutexLock lock(&v);
    posts.push_back(p);
}

void pushUserData(std::unordered_map<std::string,User> &users, User &u){
    MutexLock lock(&v);
    users.insert(std::make_pair(u.id,u));
}

void parseUserBuff(BufferQueueItem*& item, std::unordered_map<std::string,User> &users){
    char* buffer = reinterpret_cast<char*>(item->pBuffer->getBuffer());
    int length = item->pBuffer->getUsedLength();
    int len = 0;
    for(int i=0; i<length; i++){
        if(buffer[i] == '\n'){
            std::string line(&buffer[i-len],len);
            std::string id = parseFieldFromLine(line, "Id");
            std::string displayName = parseFieldFromLine(line, "DisplayName");
            if (!id.empty() && !displayName.empty()) {
                User u(id, displayName);
                pushUserData(users, u);
            }
            len = 0;
        }
        len++;
    }
}
 

void parsePostBuff(BufferQueueItem*& item, std::vector<Post> &posts){
    char* buffer = reinterpret_cast<char*>(item->pBuffer->getBuffer());
    int length = item->pBuffer->getUsedLength();
    int len = 0;
    for(int i=0; i<length; i++){
        len++;
        if(buffer[i] == '\n'){
            std::string line(&buffer[i-len-1],len-1);
            len = 0;
            std::string creationDate = parseFieldFromLine(line, "CreationDate");
            if(creationDate.empty()) continue; 
            struct std::tm creationDateTm;
            strptime(creationDate.c_str(), "%Y-%m-%dT%H:%M:%S.%f", &creationDateTm);
            if(creationDateTm.tm_year + 1900 != TARGET_YEAR) continue;
            std::string id = parseFieldFromLine(line, "Id");
            if(id.empty()) continue; 
            std::string postTypeId = parseFieldFromLine(line, "PostTypeId");
            if(postTypeId.empty()) continue; 
            std::string ownerUserId = parseFieldFromLine(line, "OwnerUserId");
            if(ownerUserId.empty()) continue; 
            int score = parseIntFromLine(line, "Score");

            Post p(id, postTypeId, ownerUserId, creationDateTm, score);
            pushPostData(posts, p);
        }
    }
}

void push2queue(BufferQueueItem* item){
    MutexLock lock(&m);
    q.push_back(item);
    pthread_cond_signal(&cv);
}

void readQueue(BufferQueueItem*& item){
    MutexLock lock(&m);
    if(q.empty())
        pthread_cond_wait(&cv, &m);
    if(q.empty()){
//        std::cout << "Signal received but queue is empty!" << std::endl;
        item = nullptr;
        return;
    }
    item = q.front();
    q.pop_front();
}

void postThreadFunc(std::vector<Post> &posts){
    while(true){
        BufferQueueItem* item = nullptr;
        readQueue(item);
        if(item == nullptr) continue;
        if(item->pBuffer->isShutdownMsg()){
            item->pBuffer->markShutdown(false);
            pool.returnToPool(item);
            break;
        }
        parsePostBuff(item, posts);
        pool.returnToPool(item);
    }
}

void userThreadFunc(std::unordered_map<std::string,User> &users){
    while(true){
        BufferQueueItem* item = nullptr;
        readQueue(item);
        if(item == nullptr) continue;
        if(item->pBuffer->isShutdownMsg()){
            item->pBuffer->markShutdown(false);
            pool.returnToPool(item);
            break;
        }
        parseUserBuff(item, users);
        pool.returnToPool(item);
    }
}

void readToBuff(const std::string &filename, bool isUser) {
    std::ifstream fin(filename.c_str(),std::ifstream::binary);
    fin.seekg (0, fin.end);
    long int length = fin.tellg();
    fin.seekg (0, fin.beg);
    int readLen = 0;
    std::string line;
    while(length > 0){
        readLen = ((length - BUFF_READ_SIZE) > 0) ? BUFF_READ_SIZE : length;
        BufferQueueItem* item = pool.getFromPool();
        char *buffer = reinterpret_cast<char*>(item->pBuffer->getBuffer());
        fin.read(buffer, readLen);
        std::getline(fin, line);
        line.push_back('\n');
        memcpy(buffer + readLen, line.c_str(), line.size());
        item->pBuffer->setLength(readLen + line.size());
        push2queue(item);
        length = length - readLen - line.size();
        line.clear();
    }
    int tCount = USER_THREAD_COUNT;
    if(!isUser) tCount = THREAD_COUNT;
    for(int i=0; i<tCount; i++){
        BufferQueueItem* item = pool.getFromPool();
        item->pBuffer->markShutdown(true);
        push2queue(item);
    }
}

const User *findUser(const std::string &id, const std::unordered_map<std::string,User> &users) {
    std::unordered_map<std::string,User>::const_iterator it = users.find (id);
    if(it != users.end())
        return &it->second;
    return NULL;
}

struct UserData {
    std::string displayName;
    long long int totalScore;
};

// Returns pair of topUsersByQuestions, topUsersByAnswers
void getTopScoringUsers(
    const std::unordered_map<std::string,User> &users,
    const std::vector<Post> &posts,
    std::map<std::string, UserData> &questions,
    std::map<std::string, UserData> &answers,
    int start, int end) {

    // Calculate the total score of questions per user
    for (const auto &p : posts) {
        const User *u = findUser(p.ownerUserId, users);
        if (u) {
            questions[u->id].displayName = u->displayName;
            if (p.postTypeId == Post::QUESTION && (p.creationDate.tm_year + 1900) == TARGET_YEAR) {
                questions[u->id].totalScore += p.score;
            }
        }
    }

    // Calculate the total score of answers per user
    for (const auto &p : posts) {
        const User *u = findUser(p.ownerUserId, users);
        if (u) {
            answers[u->id].displayName = u->displayName;
            if (p.postTypeId == Post::ANSWER && (p.creationDate.tm_year + 1900) == TARGET_YEAR) {
                answers[u->id].totalScore += p.score;
            }
        }
    }
}

int main(int argv, char** argc) {
    auto start = std::chrono::steady_clock::now();
    if (argv != 3) {
        std::cout << "ERROR: usage: " << argc[0] << " <users file>" << " <posts file>" << std::endl;
        exit(1);
    }

    // Initialize mutex, condition var and Buffers
    if (pthread_mutex_init(&m, nullptr) != 0)
        std::cout << "Failed to Initialize Mutex" << std::endl;
    if (pthread_mutex_init(&v, nullptr) != 0)
        std::cout << "Failed to Initialize Mutex" << std::endl;
    if (pthread_cond_init(&cv, nullptr) != 0)
        std::cout << "Failed to Initialize Conditional Variable" << std::endl;
    pool.init(BUFFER_SIZE, MAX_BUFFERS);

    // Keep track of all users
    std::unordered_map<std::string,User> users;

    // Keep track of all posts
    std::vector<Post> posts;
    
    for(int t=0; t<USER_THREAD_COUNT; t++){
        std::thread* name = new std::thread(userThreadFunc, std::ref(users));
        threads.push_back(name);
    }

    // Parse and load posts and user data
    std::string usersFile = argc[1];
    std::string postsFile = argc[2];
    auto start1 = std::chrono::steady_clock::now();
    readToBuff(usersFile,true);
    auto end1 = std::chrono::steady_clock::now();
    auto diff1 = end1 - start1;
    std::cout << "Reading Users File took : " << std::chrono::duration <double,std::milli> (diff1).count() << " ms" << std::endl;

    for(int t=0; t<USER_THREAD_COUNT; t++){
        threads[t]->join();
    }
    threads.clear();

    for(int t=0; t<THREAD_COUNT; t++){
        std::thread* name = new std::thread(postThreadFunc, std::ref(posts));
        threads.push_back(name);
    }

    auto start2 = std::chrono::steady_clock::now();
    readToBuff(postsFile,false);
    auto end2 = std::chrono::steady_clock::now();
    auto diff2 = end2 - start2;
    std::cout << "Reading Posts file took : " << std::chrono::duration <double,std::milli> (diff2).count() << " ms" << std::endl;

    for(int t=0; t<THREAD_COUNT; t++){
        threads[t]->join();
    }
    threads.clear();
    
    unsigned int postsLen = posts.size();
    std::cout << "Users size : " << users.size() << " Posts size : " << postsLen << std::endl;
   
    std::map<std::string, UserData> questions;
    std::map<std::string, UserData> answers;
    
    getTopScoringUsers(users, posts, questions, answers, 0, postsLen);

    std::vector<UserData> topUsersByQuestions(TOP_USERS);
    
    for (int i = 0; i < TOP_USERS; i++) {
        std::string key;
        UserData maxData = { .displayName = "No User", .totalScore = 0 };
        for (const auto& it : questions) {
            if (it.second.totalScore >= maxData.totalScore) {
                key = it.first;
                maxData = it.second;
            }
        }
        topUsersByQuestions[i] = maxData;
        questions.erase(key);
    }

    std::vector<UserData> topUsersByAnswers(TOP_USERS);
    
    std::cout << std::endl;
    for (int i = 0; i < TOP_USERS; i++) {
        std::string key;
        UserData maxData = { .displayName = "No User", .totalScore = 0 };
        for (const auto& it : answers) {
            if (it.second.totalScore >= maxData.totalScore) {
                key = it.first;
                maxData = it.second;
            }
        }
        topUsersByAnswers[i] = maxData;
        answers.erase(key);
    }

    std::cout << "Top " << TOP_USERS << " users by total score on questions asked from " << TARGET_YEAR << std::endl;
    for (const auto &q : topUsersByQuestions) {
        std::cout << q.totalScore << '\t' << q.displayName << std::endl;
    }
    std::cout << std::endl;

    std::cout << "Top " << TOP_USERS << " users by total score on answers posted from " << TARGET_YEAR << std::endl;
    for (const auto &q : topUsersByAnswers) {
        std::cout << q.totalScore << '\t' << q.displayName << std::endl;
    }
    std::cout << std::endl;

    auto end = std::chrono::steady_clock::now();
    auto diff = end - start;
    std::cout << std::chrono::duration <double,std::milli> (diff).count() << " ms" << std::endl;
    return 0;
}
