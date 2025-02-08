#include <bits/stdc++.h>
#include <mpi.h>
#include <fstream>
#include <sstream>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>

using namespace std;

const int CHUNK_SIZE = 32;
const int REPLICATION_FACTOR = 3;
const int HEARTBEAT_INTERVAL = 1; // seconds
const int FAILOVER_INTERVAL = 1; // seconds

// Data Structures
struct Chunk {
    string data;
    string previousChunk;  // Data of the previous chunk
    string nextChunk;      // Data of the next chunk
};


// Commands
struct Commands {
    static const int UPLOAD = 1;
    static const int RETRIEVE = 2;
    static const int SEARCH = 3;
    static const int TERMINATE = -1;
};

// Globals
map<string, vector<vector<int>>> metadataServer; // FileName -> Chunk locations
map<int, map<string, map<int, Chunk>>> storage; // Node-specific chunk storage
map<int, bool> nodeStatus;                      // Node status tracking
map<int, chrono::steady_clock::time_point> lastHeartbeat; // Heartbeat timestamps
map<int, bool> failoverState; // Track explicitly failed nodes
map<int, int> nodeLoad; // Global map for node load tracking
mutex metadataMutex, nodeStatusMutex, commandMutex, storageMutex, offsetsMutex, nodeLoadMutex;
atomic<bool> running(true);

// Function Declarations
vector<Chunk> partitionFile(const string &filePath);
void signalWorkersToTerminate(int size,int messageTag);
int handleLoadBalance(const vector<int> &availableNodes, set<int> &usedNodes);
void distributeChunks(const vector<Chunk> &chunks, const string &fileName, int rank, int size);
void handleUploadMaster(int rank, int size, const string &fileName, const string &filePath);
void handleUploadWorker(int rank, const string &fileName);
void handleRetrieveMaster(int rank, int size, const string &fileName);
void handleRetrieveWorker(int rank, const string &fileName);
void searchWordInChunk(const string& chunkData, const string& word, set<int>& offsets,int chunkOffset, int prevChunkSize, int currentChunkSize); 
void handleSearchMaster(int rank, int size, const string& fileName);
void handleSearchWorker(int rank, const string &fileName, const string &word);
void handlelistFile(const string &fileName);
void heartbeatMonitorMaster(int size);
void heartbeatWorker(int rank);
void listenForHeartbeats(int size);
void handleFailover(int rank,int size);
void handleRecover(int rank,int size);
void executeCommand(int rank, int size, const string &command);

// Function Definitions
vector<Chunk> partitionFile(const string &filePath) {
    vector<Chunk> chunks;
    ifstream file(filePath, ios::binary);
    if (!file.is_open()) return chunks;
    int chunkNumber = 0;
    char buffer[CHUNK_SIZE];
    while (file.read(buffer, CHUNK_SIZE) || file.gcount() > 0) {
        string data(buffer, file.gcount());
        chunks.push_back({data, "", ""});
    }
    file.close();
    for (size_t chunkNumber = 0; chunkNumber < chunks.size(); ++chunkNumber) {
        if (chunkNumber > 0) chunks[chunkNumber].previousChunk = chunks[chunkNumber - 1].data; 
        if (chunkNumber < chunks.size() - 1) chunks[chunkNumber].nextChunk = chunks[chunkNumber + 1].data;
    }
    return chunks;
}


void signalWorkersToTerminate(int size,int messageTag) {
    int terminateSignal = Commands::TERMINATE;
    for (int i = 1; i < size; ++i) MPI_Send(&terminateSignal, 1, MPI_INT, i, messageTag, MPI_COMM_WORLD);
}

int handleLoadBalance(const vector<int> &availableNodes, set<int> &usedNodes) {
    lock_guard<mutex> lock(nodeLoadMutex); 
    auto targetNode = min_element(availableNodes.begin(), availableNodes.end(), [&](int a, int b) {
        if (usedNodes.count(a)) return false;
        if (usedNodes.count(b)) return true;
        return nodeLoad[a] < nodeLoad[b];
    });
    return (targetNode != availableNodes.end() && !usedNodes.count(*targetNode)) ? *targetNode : -1;
}

void distributeChunks(const vector<Chunk> &chunks, const string &fileName, int rank, int size) {
    vector<vector<int>> chunkLocations;
    vector<int> availableNodes;
    {
        lock_guard<mutex> lock(nodeStatusMutex);
        for (int i = 1; i < size; ++i) {
            if (nodeStatus[i]) availableNodes.push_back(i);
        }
    }
    int availableNodesCount = availableNodes.size();
    int effectiveReplicationFactor = min(REPLICATION_FACTOR, availableNodesCount);

    if (effectiveReplicationFactor == 0) {
        cout << "-1\n";
        signalWorkersToTerminate(size, Commands::UPLOAD);
        return;
    }

    for (size_t chunkNumber = 0; chunkNumber < chunks.size(); ++chunkNumber) {
        const auto &chunk = chunks[chunkNumber];
        vector<int> replicas;
        set<int> usedNodes;

        string previousChunkData = (chunkNumber > 0) ? chunks[chunkNumber - 1].data : "";
        string nextChunkData = (chunkNumber < chunks.size() - 1) ? chunks[chunkNumber + 1].data : "";

        for (int i = 0; i < effectiveReplicationFactor; ++i) {
            int targetNode = handleLoadBalance(availableNodes, usedNodes);
            assert(targetNode != -1);
            int chunkSize = chunk.data.size();
            
            MPI_Send(&chunkSize, 1, MPI_INT, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            MPI_Send(chunk.data.data(), chunkSize, MPI_CHAR, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            MPI_Send(&chunkNumber, 1, MPI_INT, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            
            int previousChunkSize = previousChunkData.size();
            int nextChunkSize = nextChunkData.size();
            
            MPI_Send(&previousChunkSize, 1, MPI_INT, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            MPI_Send(previousChunkData.data(), previousChunkSize, MPI_CHAR, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            MPI_Send(&nextChunkSize, 1, MPI_INT, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            MPI_Send(nextChunkData.data(), nextChunkSize, MPI_CHAR, targetNode, Commands::UPLOAD, MPI_COMM_WORLD);
            
            replicas.push_back(targetNode);
            {
                lock_guard<mutex> lock(nodeLoadMutex);
                nodeLoad[targetNode]++;
            }
            usedNodes.insert(targetNode);
        }

        chunkLocations.push_back(replicas);
    }

    signalWorkersToTerminate(size, Commands::UPLOAD);
    {
        lock_guard<mutex> lock(metadataMutex);
        metadataServer[fileName] = chunkLocations;
    }
    cout << "1\n";
    handlelistFile(fileName);
}



void handleUploadMaster(int rank, int size, const string &fileName, const string &filePath) {
    if (filePath.size()==0 || fileName.size()==0 || filePath.size() < fileName.size() || filePath.substr(filePath.size() - fileName.size()) != fileName) {
        cout<<"-1\n";
        signalWorkersToTerminate(size, Commands::UPLOAD);
        return;
    }
    {
        lock_guard<mutex> lock(metadataMutex);
        if (metadataServer.count(fileName)) {
            cout<<"-1\n";
            signalWorkersToTerminate(size, Commands::UPLOAD);
            return;
        }
    }
    vector<Chunk> chunks = partitionFile(filePath);
    if (chunks.empty()) { //Assuming file is non-empty
        cout<<"-1\n";
        signalWorkersToTerminate(size, Commands::UPLOAD);
        return;
    }
    distributeChunks(chunks, fileName, rank, size);
}


void handleUploadWorker(int rank, const string &fileName) {
    while (true) {
        int chunkSize = 0;
        MPI_Recv(&chunkSize, 1, MPI_INT, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (chunkSize <= 0) break;

        vector<char> buffer(chunkSize);
        MPI_Recv(buffer.data(), chunkSize, MPI_CHAR, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int chunkNumber;
        MPI_Recv(&chunkNumber, 1, MPI_INT, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        int previousChunkSize;
        MPI_Recv(&previousChunkSize, 1, MPI_INT, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        vector<char> previousBuffer(previousChunkSize);
        MPI_Recv(previousBuffer.data(), previousChunkSize, MPI_CHAR, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        string previousChunk(previousBuffer.begin(), previousBuffer.end());

        int nextChunkSize;
        MPI_Recv(&nextChunkSize, 1, MPI_INT, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        vector<char> nextBuffer(nextChunkSize);
        MPI_Recv(nextBuffer.data(), nextChunkSize, MPI_CHAR, 0, Commands::UPLOAD, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        string nextChunk(nextBuffer.begin(), nextBuffer.end());

        string data(buffer.begin(), buffer.end());

        Chunk chunk{data, previousChunk, nextChunk};
        {
            lock_guard<mutex> lock(storageMutex);
            storage[rank][fileName][chunkNumber] = chunk;
        }
    }
}


void handleRetrieveMaster(int rank, int size, const string &fileName) {
    vector<vector<int>> chunkLocations;
    {
        lock_guard<mutex> lock(metadataMutex);
        if (!metadataServer.count(fileName)) {
            cout<<"-1\n";
            signalWorkersToTerminate(size, Commands::RETRIEVE);
            return;
        }
        chunkLocations = metadataServer[fileName]; 
    }
    string retrievedFile;

    for (size_t chunkNumber = 0; chunkNumber < chunkLocations.size(); ++chunkNumber) {
        bool chunkRetrieved = false;
        for (int node : chunkLocations[chunkNumber]) {
            {
                lock_guard<mutex> lock(nodeStatusMutex);
                if (!nodeStatus[node]) continue;
            }
            MPI_Send(&chunkNumber, 1, MPI_INT, node, Commands::RETRIEVE, MPI_COMM_WORLD);

            int chunkSize;
            MPI_Recv(&chunkSize, 1, MPI_INT, node, Commands::RETRIEVE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (chunkSize > 0) {
                vector<char> buffer(chunkSize);
                MPI_Recv(buffer.data(), chunkSize, MPI_CHAR, node, Commands::RETRIEVE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                retrievedFile += string(buffer.begin(), buffer.end());
                chunkRetrieved = true;
                break;
            }
            
        }
        if (!chunkRetrieved) {
            cout<<"-1\n";
            signalWorkersToTerminate(size, Commands::RETRIEVE);
            return;
        }
    }
    signalWorkersToTerminate(size, Commands::RETRIEVE);
    cout << retrievedFile << endl;
}

void handleRetrieveWorker(int rank, const string &fileName) {
    while (true) {
        int chunkNumber;
        MPI_Recv(&chunkNumber, 1, MPI_INT, 0, Commands::RETRIEVE, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        if (chunkNumber < 0) break;
        bool chunkFound = false;
        {
            lock_guard<mutex> lock(storageMutex);
            if (storage[rank].count(fileName) && storage[rank][fileName].count(chunkNumber)) {
                const auto &chunk=storage[rank][fileName][chunkNumber];
                int chunkSize = chunk.data.size();
                MPI_Send(&chunkSize, 1, MPI_INT, 0, Commands::RETRIEVE, MPI_COMM_WORLD);
                if (chunkSize > 0) MPI_Send(chunk.data.data(), chunkSize, MPI_CHAR, 0, Commands::RETRIEVE, MPI_COMM_WORLD);
                chunkFound = true;
            }
        }
        if (!chunkFound) {
            int chunkSize = 0;
            MPI_Send(&chunkSize, 1, MPI_INT, 0, Commands::RETRIEVE, MPI_COMM_WORLD);
        }
    }
}

void searchWordInChunk(const string& chunkData, const string& word, set<int>& offsets, 
                       int chunkOffset, int prevChunkSize, int currentChunkSize) {
    size_t pos = prevChunkSize;
    while (pos < prevChunkSize + currentChunkSize && 
           (pos = chunkData.find(word, pos)) != string::npos) {
        bool isCompleteWord = true;
        if (pos > 0 && chunkData[pos - 1] != ' ') isCompleteWord = false;
        if (pos + word.size() < chunkData.size() && chunkData[pos + word.size()] != ' ') isCompleteWord = false;
        if (isCompleteWord) {
            lock_guard<mutex> lock(offsetsMutex);
            offsets.insert(chunkOffset + pos); 
        }
        pos += word.size();
    }
}

void handleSearchWorker(int rank, const string& fileName, const string& word) {
    int isActive = 0;
    MPI_Bcast(&isActive, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (!isActive) return;
    {
        lock_guard<mutex> lock(nodeStatusMutex);
        if (!nodeStatus[rank]) return;   
    }
    set<int> localOffsets;
    {
        lock_guard<mutex> lock(storageMutex);
        for (const auto& [chunkNumber, chunk] : storage[rank][fileName]) {
            string previousChunk = chunk.previousChunk;
            string currentChunk = chunk.data;
            string nextChunk = chunk.nextChunk;
            string mergedChunk = previousChunk+currentChunk+nextChunk;
            searchWordInChunk(mergedChunk, word, localOffsets, chunkNumber * CHUNK_SIZE - previousChunk.size(),
                              previousChunk.size(), currentChunk.size());
        }
    }
    int searchResult = localOffsets.size();
    if(word.size()==0 || word.size()>CHUNK_SIZE) searchResult=-1;
    MPI_Send(&searchResult, 1, MPI_INT, 0, Commands::SEARCH, MPI_COMM_WORLD);
    if (searchResult>0) {
        vector<int> offsetsVector(localOffsets.begin(), localOffsets.end());
        MPI_Send(offsetsVector.data(), offsetsVector.size(), MPI_INT, 0, Commands::SEARCH, MPI_COMM_WORLD);
    }
}

// Master function: Aggregate results from all workers
void handleSearchMaster(int rank, int size, const string& fileName) {
    lock_guard<mutex> lock(metadataMutex);
    if (!metadataServer.count(fileName)) {
        cout << "-1\n";
        int isActive = 0;
        MPI_Bcast(&isActive, 1, MPI_INT, 0, MPI_COMM_WORLD);
        return;
    }
    const auto &chunkLocations = metadataServer[fileName]; //Ensure there exists at least single activeReplica for every chunk
    for (size_t chunkNumber = 0; chunkNumber < chunkLocations.size(); ++chunkNumber) {
        const auto &replicas = chunkLocations[chunkNumber];
        int cntActiveReplicas=0;
        for (const auto &node : replicas) {
            lock_guard<mutex> lock(nodeStatusMutex);
            if (nodeStatus[node]) cntActiveReplicas++;
        }
        if(cntActiveReplicas==0){
            cout<<"-1\n";
            int isActive = 0;
            MPI_Bcast(&isActive, 1, MPI_INT, 0, MPI_COMM_WORLD);
            return;
        }
    }
    bool searchFailed=false;
    int isActive = 1;
    MPI_Bcast(&isActive, 1, MPI_INT, 0, MPI_COMM_WORLD);
    set<int> globalOffsets;
    for (int worker = 1; worker < size; ++worker) {
        {
            lock_guard<mutex> lock(nodeStatusMutex);
            if (!nodeStatus[worker]) continue;
        }
        int searchResult;
        MPI_Recv(&searchResult, 1, MPI_INT, worker, Commands::SEARCH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if(searchResult==-1) searchFailed=true;
        if (searchResult > 0) {
            vector<int> workerOffsets(searchResult);
            MPI_Recv(workerOffsets.data(), searchResult, MPI_INT, worker, Commands::SEARCH, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            globalOffsets.insert(workerOffsets.begin(), workerOffsets.end());
        }
    }
    if(searchFailed){
        cout<<"-1\n"; return;
    }
    cout << globalOffsets.size() << endl;
    for (int offset : globalOffsets) cout << offset << " ";
    cout << endl;
}

void handlelistFile(const string &fileName) {
    lock_guard<mutex> lock(metadataMutex);
    lock_guard<mutex> nodeStatusLock(nodeStatusMutex);
    if (!metadataServer.count(fileName)) {
        cout<<"-1\n";
        return;
    }
    const auto &chunkLocations = metadataServer[fileName];
    for (size_t chunkNumber = 0; chunkNumber < chunkLocations.size(); ++chunkNumber) {
        const auto &replicas = chunkLocations[chunkNumber];
        vector<int> activeReplicas;
        for (const auto &node : replicas) if (nodeStatus[node]) activeReplicas.push_back(node);
        cout << chunkNumber << " " << activeReplicas.size();
        sort(activeReplicas.begin(),activeReplicas.end());
        for (const auto &nodeRank : activeReplicas) cout << " " << nodeRank;
        cout << endl;
    }
}

void heartbeatWorker(int rank) {
    bool isFailover = false;
    while (running) {
        {
            unique_lock<mutex> lock(nodeStatusMutex);
            if (failoverState[rank]) isFailover = true;
            else isFailover = false;
        }
        int heartbeat = isFailover ? 0 : 1;
        this_thread::sleep_for(chrono::seconds(HEARTBEAT_INTERVAL));
        MPI_Send(&heartbeat, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
}

void handleFailover(int rank, int size) {
    if (rank == 0 || rank >= size) {
        cout << "-1\n";
        return;
    }
    lock_guard<mutex> lock(nodeStatusMutex);
    if (!nodeStatus[rank]) {
        cout << "-1\n";
        return;
    }
    failoverState[rank] = true;
    cout << "1\n";
}

void handleRecover(int rank, int size) {
    if (rank == 0 || rank >= size) {
        cout << "-1\n";
        return;
    }
    lock_guard<mutex> lock(nodeStatusMutex);
    if (nodeStatus[rank] && !failoverState[rank]) {
        cout << "-1\n";
        return;
    }
    failoverState[rank] = false;
    cout << "1\n";
}

void heartbeatMonitorMaster(int size) {
    while (running) {
        this_thread::sleep_for(chrono::seconds(HEARTBEAT_INTERVAL)); // Check every second
        lock_guard<mutex> lock(nodeStatusMutex);
        auto currentTime = chrono::steady_clock::now();
        for (int i = 1; i < size; ++i) {
            if (nodeStatus[i] && chrono::duration_cast<chrono::seconds>(currentTime - lastHeartbeat[i]).count() > FAILOVER_INTERVAL) {
                nodeStatus[i] = false; 
            }
        }
    }
}

void listenForHeartbeats(int size) {
    while (running) {
        MPI_Status status;
        int heartbeat;
        MPI_Recv(&heartbeat, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int sender = status.MPI_SOURCE;
        {
            lock_guard<mutex> lock(nodeStatusMutex);
            if (failoverState[sender]) continue;
            lastHeartbeat[sender] = chrono::steady_clock::now();
            nodeStatus[sender] = (heartbeat == 1);
        }
    }
}

// Command Execution
void executeCommand(int rank, int size, const string &command) {
    lock_guard<mutex> lock(commandMutex);
    if (command == "exit") {
        running = false;
        return;
    }
    string fileName, filePath;
    if (command.rfind("upload", 0) == 0) {
        istringstream iss(command);
        string cmd;
        iss >> cmd >> fileName >> filePath;
        if (rank == 0) handleUploadMaster(rank, size, fileName, filePath);
        else handleUploadWorker(rank, fileName);

    } else if (command.rfind("retrieve", 0) == 0) {
        istringstream iss(command);
        string cmd;
        iss >> cmd >> fileName;
        if (rank == 0) handleRetrieveMaster(rank, size, fileName);
        else handleRetrieveWorker(rank, fileName);

    } else if (command.rfind("search", 0) == 0) {
        istringstream iss(command);
        string cmd, word;
        iss >> cmd >> fileName >> word;
        if (rank == 0) handleSearchMaster(rank, size, fileName);
        else handleSearchWorker(rank, fileName, word);
        
    } else if (command.rfind("list_file", 0) == 0) {
        istringstream iss(command);
        string cmd;
        iss >> cmd >> fileName;
        if (rank == 0) handlelistFile(fileName);

    } else if (command.rfind("failover", 0) == 0) {
        istringstream iss(command);
        string cmd;
        int RANK;
        iss >> cmd >> RANK;
        if (rank == 0) handleFailover(RANK,size);

    } else if (command.rfind("recover", 0) == 0) {
        istringstream iss(command);
        string cmd;
        int RANK;
        iss >> cmd >> RANK;
        if (rank == 0) handleRecover(RANK,size);

    } else if(rank==1) cout<<"-1\n";
}

int main(int argc, char **argv) {
    int rank, size;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &rank);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    {
        lock_guard<mutex> lock(nodeStatusMutex);
        auto currentTime = chrono::steady_clock::now();
        for (int i = 1; i < size; ++i) {
            nodeStatus[i] = true;
            lastHeartbeat[i] = currentTime;
        }
    }
    thread heartbeatMonitorThread, heartbeatListenerThread, heartbeatWorkerThread;
    
    if(rank==0){
        heartbeatMonitorThread = thread(heartbeatMonitorMaster, size);
        heartbeatListenerThread = thread(listenForHeartbeats, size);
    } else heartbeatWorkerThread = thread(heartbeatWorker, rank);
    
    string command;
    while (running) {
        if (rank == 0) getline(cin, command);
        int commandLength = command.size();
        MPI_Bcast(&commandLength, 1, MPI_INT, 0, MPI_COMM_WORLD);
        command.resize(commandLength); 
        MPI_Bcast(command.data(), commandLength, MPI_CHAR, 0, MPI_COMM_WORLD);
        executeCommand(rank, size, command);
    }
    if (rank == 0) {
        if (heartbeatMonitorThread.joinable()) heartbeatMonitorThread.join();
        if (heartbeatListenerThread.joinable()) heartbeatListenerThread.join();
    } else if (heartbeatWorkerThread.joinable()) heartbeatWorkerThread.join();
    
    MPI_Finalize();
    return 0;
}
