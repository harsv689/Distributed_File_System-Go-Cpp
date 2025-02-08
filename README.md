# Open-Source Distributed File System (OsDFS)

## Overview
This Distributed File System (OsDFS) is a robust and resilient Distributed File System (DFS) designed to ensure redundancy and reliability in storing critical data. The system is implemented using MPI, where:
- **Rank 0** acts as the central metadata server.
- **Ranks 1 to N-1** serve as storage nodes.

The system supports file uploads, downloads, distributed search, load balancing, and failover and recovery mechanisms.

---
## Features
### 1. File Partitioning & Three-Way Replication
- Files are split into fixed-size chunks (32 bytes each).
- Each chunk is stored on three different nodes for redundancy.
- No requirement to maintain replication in case of failures.

### 2. Load Balancing
- Chunks are evenly distributed across nodes.
- No node stores more than one replica of the same chunk.

### 3. Heartbeat Mechanism & Failure Detection
- Each node periodically sends a heartbeat signal to the metadata server.
- If a node fails to send a heartbeat for more than 3 seconds, it is marked as down.

### 4. Failover & Recovery Operations
- **Failover**: Simulates the failure of a storage node, stopping requests and heartbeats.
- **Recovery**: Restores a failed node and its ability to process requests and send heartbeats.

### 5. File Operations
- **Upload**: Files are partitioned and stored with replication.
- **Download**: Chunks are retrieved and reassembled.
- **Search**: Locates file chunks containing a specific word.

### 6. Error Handling
- If an error occurs during operations, output `-1`.
- Examples: Searching for a non-existent file, downloading a file with missing chunks, etc.

---
## Commands & Input Format
Commands are taken from **stdin**:
- `upload <file_name> <absolute_file_path>` – Uploads a file to ADFS.
- `retrieve <file_name>` – Downloads a file and reassembles it.
- `search <file_name> <word>` – Searches for a word in a file.
- `list file <file_name>` – Lists nodes storing chunks of a file.
- `failover <rank>` – Simulates failure of a node.
- `recover <rank>` – Restores a previously failed node.
- `exit` – Terminates the program.

---
## Output Format
- Success outputs `1`.
- Errors output `-1`.
- Specific outputs for commands such as `list file`, `search`, and `retrieve`.

---
## Additional Notes
- The metadata server (Rank 0) is always operational.
- Maximum file size: ~1-2MB.
- Heartbeat messages sent every 1-2 seconds.
- Search functionality provides exact word matches only.

---
## Example Usage
### **Test Case 1** (File Upload & Retrieval)
#### **Input:**
```
upload a.txt testcases/a.txt
list file a.txt
failover 1
failover 2
retrieve a.txt
search a.txt abcd
exit
```
#### **Output:**
```
1
0 3 1 2 3
1 3 1 2 4
2 3 1 3 4
0 3 1 2 3
1 3 1 2 4
2 3 1 3 4
1
1
abcd efgh ijkl abcd abcd defg dsds abcddufhef ebhfeihf udefheifh abcde
3
0 15 20
```

### **Test Case 2** (Failover & Recovery)
#### **Input:**
```
upload a.txt testcases/a.txt
failover 1
list file a.txt
recover 1
list file a.txt
exit
```
#### **Output:**
```
1
0 3 1 2 3
1 3 1 2 4
2 3 1 3 4
1
0 2 2 3
1 2 2 4
2 2 3 4
1
0 3 1 2 3
1 3 1 2 4
2 3 1 3 4
```

---
## Implementation Details
### **1. Metadata Server (Iron Man - Rank 0)**
- Maintains a mapping of file chunks to storage nodes.
- Handles requests for uploads, downloads, and searches.
- Monitors the health of storage nodes through heartbeats.

### **2. Storage Nodes (Avengers - Rank 1 to N-1)**
- Store file chunks assigned by the metadata server.
- Respond to retrieval and search requests.
- Send periodic heartbeat messages to the metadata server.

### **3. Failover & Recovery Handling**
- Nodes marked as failed do not respond to any requests.
- Recovery is simulated by restoring a node’s ability to process requests and send heartbeats.

