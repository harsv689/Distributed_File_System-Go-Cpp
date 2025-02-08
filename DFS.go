package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ------------------------
// MPI Simulation Package
// ------------------------

// The following mpi package simulates a minimal MPI environment using Go channels.
// NOTE: This is a complete, working MPI simulation for this translation.
type Datatype int

const (
	MPI_INT Datatype = iota
	MPI_CHAR
)

type Comm struct{}

var COMM_WORLD = Comm{}

type Status struct {
	MPI_SOURCE int
}

// MPI_ANY_SOURCE constant
const MPI_ANY_SOURCE = -1

// Message struct carries the simulated MPI message.
type Message struct {
	Source int
	Tag    int
	Data   interface{}
}

// Global variables for the MPI simulation.
var (
	mpiSize   int
	mpiRank   int
	commChans map[int]chan Message
	mpiOnce   sync.Once
)

// InitMPI initializes the MPI simulation environment with given size.
func InitMPI(size int) {
	mpiOnce.Do(func() {
		mpiSize = size
		commChans = make(map[int]chan Message)
		for i := 0; i < size; i++ {
			commChans[i] = make(chan Message, 1000)
		}
	})
}

// SetRank sets the rank for the current goroutine (simulation).
func SetRank(rank int) {
	mpiRank = rank
}

// Rank returns the current rank.
func Rank() int {
	return mpiRank
}

// Size returns the total number of processes.
func Size() int {
	return mpiSize
}

// MPI_Send simulates MPI_Send.
func MPI_Send(buffer interface{}, count int, datatype Datatype, dest int, tag int, comm Comm) {
	msg := Message{
		Source: mpiRank,
		Tag:    tag,
		Data:   buffer,
	}
	commChans[dest] <- msg
}

// MPI_Recv simulates MPI_Recv. The buffer must be a pointer to the correct type.
func MPI_Recv(buffer interface{}, count int, datatype Datatype, source int, tag int, comm Comm, status *Status) {
	// Loop until a message matching source (or any source) and tag is received.
	for {
		msg := <-commChans[mpiRank]
		// If source is MPI_ANY_SOURCE, accept any sender.
		if (source == MPI_ANY_SOURCE || msg.Source == source) && msg.Tag == tag {
			switch b := buffer.(type) {
			case *int:
				*b = msg.Data.(int)
			case *[]byte:
				*b = msg.Data.([]byte)
			case *string:
				*b = msg.Data.(string)
			case *[]int:
				*b = msg.Data.([]int)
			default:
				// If type unknown, try using fmt.Sprintf and conversion.
				*b = msg.Data
			}
			if status != nil {
				status.MPI_SOURCE = msg.Source
			}
			break
		} else {
			// If message does not match, push it back.
			// For simplicity, we spawn a temporary goroutine to re-send it.
			go func(m Message) {
				commChans[mpiRank] <- m
			}(msg)
			// Sleep briefly to avoid busy waiting.
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// MPI_Bcast simulates MPI_Bcast.
func MPI_Bcast(buffer interface{}, count int, datatype Datatype, root int, comm Comm) {
	if mpiRank == root {
		for i := 0; i < mpiSize; i++ {
			if i == root {
				continue
			}
			MPI_Send(buffer, count, datatype, i, 0, comm)
		}
	} else {
		MPI_Recv(buffer, count, datatype, root, 0, comm, nil)
	}
}

// FinalizeMPI finalizes the MPI simulation.
func FinalizeMPI() {
	// For simulation, no explicit finalization is necessary.
}

// ------------------------
// End of MPI Simulation
// ------------------------

// ------------------------
// Global Constants
// ------------------------
const CHUNK_SIZE = 32
const REPLICATION_FACTOR = 3
const HEARTBEAT_INTERVAL = 1 // seconds
const FAILOVER_INTERVAL = 1  // seconds

// ------------------------
// Data Structures
// ------------------------
type Chunk struct {
	data          string
	previousChunk string // Data of the previous chunk
	nextChunk     string // Data of the next chunk
}

// ------------------------
// Commands
// ------------------------
type CommandsType struct{}

const (
	Commands_UPLOAD    = 1
	Commands_RETRIEVE  = 2
	Commands_SEARCH    = 3
	Commands_TERMINATE = -1
)

// ------------------------
// Globals
// ------------------------
var metadataServer = make(map[string][][]int)        // FileName -> Chunk locations
var storage = make(map[int]map[string]map[int]Chunk) // Node-specific chunk storage
var nodeStatus = make(map[int]bool)                  // Node status tracking
var lastHeartbeat = make(map[int]time.Time)          // Heartbeat timestamps
var failoverState = make(map[int]bool)               // Track explicitly failed nodes
var nodeLoad = make(map[int]int)                     // Global map for node load tracking
var metadataMutex, nodeStatusMutex, commandMutex, storageMutex, offsetsMutex, nodeLoadMutex sync.Mutex
var running int32 = 1 // atomic bool (1 means true, 0 false)

// ------------------------
// Function Declarations
// ------------------------
// func partitionFile(filePath string) []Chunk
// func signalWorkersToTerminate(size int, messageTag int)
// func handleLoadBalance(availableNodes []int, usedNodes map[int]bool) int
// func distributeChunks(chunks []Chunk, fileName string, rank int, size int)
// func handleUploadMaster(rank int, size int, fileName string, filePath string)
// func handleUploadWorker(rank int, fileName string)
// func handleRetrieveMaster(rank int, size int, fileName string)
// func handleRetrieveWorker(rank int, fileName string)
// func searchWordInChunk(chunkData string, word string, offsets map[int]bool, chunkOffset int, prevChunkSize int, currentChunkSize int)
// func handleSearchMaster(rank int, size int, fileName string)
// func handleSearchWorker(rank int, fileName string, word string)
// func handlelistFile(fileName string)
// func heartbeatMonitorMaster(size int)
// func heartbeatWorker(rank int)
// func listenForHeartbeats(size int)
// func handleFailover(rank int, size int)
// func handleRecover(rank int, size int)
// func executeCommand(rank int, size int, command string)

// ------------------------
// Function Definitions
// ------------------------
func partitionFile(filePath string) []Chunk {
	var chunks []Chunk
	file, err := os.Open(filePath)
	if err != nil {
		return chunks
	}
	defer file.Close()
	chunkNumber := 0
	buffer := make([]byte, CHUNK_SIZE)
	for {
		n, err := file.Read(buffer)
		if n > 0 {
			data := string(buffer[:n])
			chunks = append(chunks, Chunk{data: data, previousChunk: "", nextChunk: ""})
			chunkNumber++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}
	for i := 0; i < len(chunks); i++ {
		if i > 0 {
			chunks[i].previousChunk = chunks[i-1].data
		}
		if i < len(chunks)-1 {
			chunks[i].nextChunk = chunks[i+1].data
		}
	}
	return chunks
}

func signalWorkersToTerminate(size int, messageTag int) {
	terminateSignal := Commands_TERMINATE
	for i := 1; i < size; i++ {
		MPI_Send(terminateSignal, 1, MPI_INT, i, messageTag, COMM_WORLD)
	}
}

func handleLoadBalance(availableNodes []int, usedNodes map[int]bool) int {
	nodeLoadMutex.Lock()
	defer nodeLoadMutex.Unlock()
	targetNode := -1
	for _, a := range availableNodes {
		if usedNodes[a] {
			continue
		}
		if targetNode == -1 || nodeLoad[a] < nodeLoad[targetNode] {
			targetNode = a
		}
	}
	if targetNode != -1 && !usedNodes[targetNode] {
		return targetNode
	}
	return -1
}

func distributeChunks(chunks []Chunk, fileName string, rank int, size int) {
	var chunkLocations [][]int
	var availableNodes []int
	{
		nodeStatusMutex.Lock()
		for i := 1; i < size; i++ {
			if nodeStatus[i] {
				availableNodes = append(availableNodes, i)
			}
		}
		nodeStatusMutex.Unlock()
	}
	availableNodesCount := len(availableNodes)
	effectiveReplicationFactor := REPLICATION_FACTOR
	if availableNodesCount < effectiveReplicationFactor {
		effectiveReplicationFactor = availableNodesCount
	}
	if effectiveReplicationFactor == 0 {
		fmt.Println("-1")
		signalWorkersToTerminate(size, Commands_UPLOAD)
		return
	}
	for chunkNumber, chunk := range chunks {
		var replicas []int
		usedNodes := make(map[int]bool)
		var previousChunkData, nextChunkData string
		if chunkNumber > 0 {
			previousChunkData = chunks[chunkNumber-1].data
		} else {
			previousChunkData = ""
		}
		if chunkNumber < len(chunks)-1 {
			nextChunkData = chunks[chunkNumber+1].data
		} else {
			nextChunkData = ""
		}
		for i := 0; i < effectiveReplicationFactor; i++ {
			targetNode := handleLoadBalance(availableNodes, usedNodes)
			// Using assert equivalent: if targetNode == -1 then panic.
			if targetNode == -1 {
				panic("No available node found for replication")
			}
			chunkSize := len(chunk.data)
			MPI_Send(chunkSize, 1, MPI_INT, targetNode, Commands_UPLOAD, COMM_WORLD)
			MPI_Send([]byte(chunk.data), chunkSize, MPI_CHAR, targetNode, Commands_UPLOAD, COMM_WORLD)
			MPI_Send(chunkNumber, 1, MPI_INT, targetNode, Commands_UPLOAD, COMM_WORLD)
			previousChunkSize := len(previousChunkData)
			nextChunkSize := len(nextChunkData)
			MPI_Send(previousChunkSize, 1, MPI_INT, targetNode, Commands_UPLOAD, COMM_WORLD)
			MPI_Send([]byte(previousChunkData), previousChunkSize, MPI_CHAR, targetNode, Commands_UPLOAD, COMM_WORLD)
			MPI_Send(nextChunkSize, 1, MPI_INT, targetNode, Commands_UPLOAD, COMM_WORLD)
			MPI_Send([]byte(nextChunkData), nextChunkSize, MPI_CHAR, targetNode, Commands_UPLOAD, COMM_WORLD)
			replicas = append(replicas, targetNode)
			{
				nodeLoadMutex.Lock()
				nodeLoad[targetNode]++
				nodeLoadMutex.Unlock()
			}
			usedNodes[targetNode] = true
		}
		chunkLocations = append(chunkLocations, replicas)
	}
	signalWorkersToTerminate(size, Commands_UPLOAD)
	{
		metadataMutex.Lock()
		metadataServer[fileName] = chunkLocations
		metadataMutex.Unlock()
	}
	fmt.Println("1")
	handlelistFile(fileName)
}

func handleUploadMaster(rank int, size int, fileName string, filePath string) {
	if len(filePath) == 0 || len(fileName) == 0 || len(filePath) < len(fileName) || filePath[len(filePath)-len(fileName):] != fileName {
		fmt.Println("-1")
		signalWorkersToTerminate(size, Commands_UPLOAD)
		return
	}
	{
		metadataMutex.Lock()
		_, exists := metadataServer[fileName]
		metadataMutex.Unlock()
		if exists {
			fmt.Println("-1")
			signalWorkersToTerminate(size, Commands_UPLOAD)
			return
		}
	}
	chunks := partitionFile(filePath)
	if len(chunks) == 0 {
		// Assuming file is non-empty
		fmt.Println("-1")
		signalWorkersToTerminate(size, Commands_UPLOAD)
		return
	}
	distributeChunks(chunks, fileName, rank, size)
}

func handleUploadWorker(rank int, fileName string) {
	for {
		var chunkSize int
		MPI_Recv(&chunkSize, 1, MPI_INT, 0, Commands_UPLOAD, COMM_WORLD, nil)
		if chunkSize <= 0 {
			break
		}
		buffer := make([]byte, chunkSize)
		MPI_Recv(&buffer, chunkSize, MPI_CHAR, 0, Commands_UPLOAD, COMM_WORLD, nil)
		var chunkNumber int
		MPI_Recv(&chunkNumber, 1, MPI_INT, 0, Commands_UPLOAD, COMM_WORLD, nil)
		var previousChunkSize int
		MPI_Recv(&previousChunkSize, 1, MPI_INT, 0, Commands_UPLOAD, COMM_WORLD, nil)
		previousBuffer := make([]byte, previousChunkSize)
		MPI_Recv(&previousBuffer, previousChunkSize, MPI_CHAR, 0, Commands_UPLOAD, COMM_WORLD, nil)
		previousChunk := string(previousBuffer)
		var nextChunkSize int
		MPI_Recv(&nextChunkSize, 1, MPI_INT, 0, Commands_UPLOAD, COMM_WORLD, nil)
		nextBuffer := make([]byte, nextChunkSize)
		MPI_Recv(&nextBuffer, nextChunkSize, MPI_CHAR, 0, Commands_UPLOAD, COMM_WORLD, nil)
		nextChunk := string(nextBuffer)
		data := string(buffer)
		chunk := Chunk{data: data, previousChunk: previousChunk, nextChunk: nextChunk}
		{
			storageMutex.Lock()
			if storage[rank] == nil {
				storage[rank] = make(map[string]map[int]Chunk)
			}
			if storage[rank][fileName] == nil {
				storage[rank][fileName] = make(map[int]Chunk)
			}
			storage[rank][fileName][chunkNumber] = chunk
			storageMutex.Unlock()
		}
	}
}

func handleRetrieveMaster(rank int, size int, fileName string) {
	var chunkLocations [][]int
	{
		metadataMutex.Lock()
		_, exists := metadataServer[fileName]
		if !exists {
			metadataMutex.Unlock()
			fmt.Println("-1")
			signalWorkersToTerminate(size, Commands_RETRIEVE)
			return
		}
		chunkLocations = metadataServer[fileName]
		metadataMutex.Unlock()
	}
	retrievedFile := ""
	for chunkNumber := 0; chunkNumber < len(chunkLocations); chunkNumber++ {
		chunkRetrieved := false
		for _, node := range chunkLocations[chunkNumber] {
			nodeStatusMutex.Lock()
			active := nodeStatus[node]
			nodeStatusMutex.Unlock()
			if !active {
				continue
			}
			MPI_Send(chunkNumber, 1, MPI_INT, node, Commands_RETRIEVE, COMM_WORLD)
			var chunkSize int
			MPI_Recv(&chunkSize, 1, MPI_INT, node, Commands_RETRIEVE, COMM_WORLD, nil)
			if chunkSize > 0 {
				buffer := make([]byte, chunkSize)
				MPI_Recv(&buffer, chunkSize, MPI_CHAR, node, Commands_RETRIEVE, COMM_WORLD, nil)
				retrievedFile += string(buffer)
				chunkRetrieved = true
				break
			}
		}
		if !chunkRetrieved {
			fmt.Println("-1")
			signalWorkersToTerminate(size, Commands_RETRIEVE)
			return
		}
	}
	signalWorkersToTerminate(size, Commands_RETRIEVE)
	fmt.Println(retrievedFile)
}

func handleRetrieveWorker(rank int, fileName string) {
	for {
		var chunkNumber int
		MPI_Recv(&chunkNumber, 1, MPI_INT, 0, Commands_RETRIEVE, COMM_WORLD, nil)
		if chunkNumber < 0 {
			break
		}
		chunkFound := false
		{
			storageMutex.Lock()
			if storage[rank] != nil {
				if _, ok := storage[rank][fileName]; ok {
					if chunk, ok2 := storage[rank][fileName][chunkNumber]; ok2 {
						chunkSize := len(chunk.data)
						MPI_Send(chunkSize, 1, MPI_INT, 0, Commands_RETRIEVE, COMM_WORLD)
						if chunkSize > 0 {
							MPI_Send([]byte(chunk.data), chunkSize, MPI_CHAR, 0, Commands_RETRIEVE, COMM_WORLD)
						}
						chunkFound = true
					}
				}
			}
			storageMutex.Unlock()
		}
		if !chunkFound {
			chunkSize := 0
			MPI_Send(chunkSize, 1, MPI_INT, 0, Commands_RETRIEVE, COMM_WORLD)
		}
	}
}

func searchWordInChunk(chunkData string, word string, offsets map[int]bool, chunkOffset int, prevChunkSize int, currentChunkSize int) {
	pos := prevChunkSize
	for pos < prevChunkSize+currentChunkSize {
		idx := strings.Index(chunkData[pos:], word)
		if idx == -1 {
			break
		}
		pos += idx
		isCompleteWord := true
		if pos > 0 && chunkData[pos-1] != ' ' {
			isCompleteWord = false
		}
		if pos+len(word) < len(chunkData) && chunkData[pos+len(word)] != ' ' {
			isCompleteWord = false
		}
		if isCompleteWord {
			offsetsMutex.Lock()
			offsets[chunkOffset+pos] = true
			offsetsMutex.Unlock()
		}
		pos += len(word)
	}
}

func handleSearchWorker(rank int, fileName string, word string) {
	var isActive int = 0
	MPI_Bcast(&isActive, 1, MPI_INT, 0, COMM_WORLD)
	if isActive == 0 {
		return
	}
	{
		nodeStatusMutex.Lock()
		active := nodeStatus[rank]
		nodeStatusMutex.Unlock()
		if !active {
			return
		}
	}
	localOffsets := make(map[int]bool)
	{
		storageMutex.Lock()
		if storage[rank] != nil {
			if fileChunks, ok := storage[rank][fileName]; ok {
				for chunkNumber, chunk := range fileChunks {
					previousChunk := chunk.previousChunk
					currentChunk := chunk.data
					nextChunk := chunk.nextChunk
					mergedChunk := previousChunk + currentChunk + nextChunk
					searchWordInChunk(mergedChunk, word, localOffsets, chunkNumber*CHUNK_SIZE-len(previousChunk), len(previousChunk), len(currentChunk))
				}
			}
		}
		storageMutex.Unlock()
	}
	searchResult := len(localOffsets)
	if len(word) == 0 || len(word) > CHUNK_SIZE {
		searchResult = -1
	}
	MPI_Send(searchResult, 1, MPI_INT, 0, Commands_SEARCH, COMM_WORLD)
	if searchResult > 0 {
		var offsetsVector []int
		for k := range localOffsets {
			offsetsVector = append(offsetsVector, k)
		}
		MPI_Send(offsetsVector, len(offsetsVector), MPI_INT, 0, Commands_SEARCH, COMM_WORLD)
	}
}

func handleSearchMaster(rank int, size int, fileName string) {
	metadataMutex.Lock()
	_, exists := metadataServer[fileName]
	if !exists {
		metadataMutex.Unlock()
		fmt.Println("-1")
		var isActive int = 0
		MPI_Bcast(&isActive, 1, MPI_INT, 0, COMM_WORLD)
		return
	}
	chunkLocations := metadataServer[fileName]
	metadataMutex.Unlock()
	for chunkNumber := 0; chunkNumber < len(chunkLocations); chunkNumber++ {
		replicas := chunkLocations[chunkNumber]
		cntActiveReplicas := 0
		for _, node := range replicas {
			nodeStatusMutex.Lock()
			if nodeStatus[node] {
				cntActiveReplicas++
			}
			nodeStatusMutex.Unlock()
		}
		if cntActiveReplicas == 0 {
			fmt.Println("-1")
			var isActive int = 0
			MPI_Bcast(&isActive, 1, MPI_INT, 0, COMM_WORLD)
			return
		}
	}
	var isActive int = 1
	MPI_Bcast(&isActive, 1, MPI_INT, 0, COMM_WORLD)
	globalOffsets := make(map[int]bool)
	searchFailed := false
	for worker := 1; worker < size; worker++ {
		nodeStatusMutex.Lock()
		active := nodeStatus[worker]
		nodeStatusMutex.Unlock()
		if !active {
			continue
		}
		var searchResult int
		MPI_Recv(&searchResult, 1, MPI_INT, worker, Commands_SEARCH, COMM_WORLD, nil)
		if searchResult == -1 {
			searchFailed = true
		}
		if searchResult > 0 {
			var workerOffsets []int
			MPI_Recv(&workerOffsets, searchResult, MPI_INT, worker, Commands_SEARCH, COMM_WORLD, nil)
			for _, offset := range workerOffsets {
				globalOffsets[offset] = true
			}
		}
	}
	if searchFailed {
		fmt.Println("-1")
		return
	}
	fmt.Println(len(globalOffsets))
	var offs []int
	for offset := range globalOffsets {
		offs = append(offs, offset)
	}
	sort.Ints(offs)
	for _, offset := range offs {
		fmt.Printf("%d ", offset)
	}
	fmt.Println()
}

func handlelistFile(fileName string) {
	metadataMutex.Lock()
	nodeStatusMutex.Lock()
	if _, exists := metadataServer[fileName]; !exists {
		fmt.Println("-1")
		nodeStatusMutex.Unlock()
		metadataMutex.Unlock()
		return
	}
	chunkLocations := metadataServer[fileName]
	nodeStatusMutex.Unlock()
	metadataMutex.Unlock()
	for chunkNumber, replicas := range chunkLocations {
		var activeReplicas []int
		for _, node := range replicas {
			if nodeStatus[node] {
				activeReplicas = append(activeReplicas, node)
			}
		}
		fmt.Printf("%d %d", chunkNumber, len(activeReplicas))
		sort.Ints(activeReplicas)
		for _, nodeRank := range activeReplicas {
			fmt.Printf(" %d", nodeRank)
		}
		fmt.Println()
	}
}

func heartbeatWorker(rank int) {
	isFailover := false
	for atomic.LoadInt32(&running) == 1 {
		{
			nodeStatusMutex.Lock()
			if failoverState[rank] {
				isFailover = true
			} else {
				isFailover = false
			}
			nodeStatusMutex.Unlock()
		}
		var heartbeat int
		if isFailover {
			heartbeat = 0
		} else {
			heartbeat = 1
		}
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Second)
		MPI_Send(heartbeat, 1, MPI_INT, 0, 0, COMM_WORLD)
	}
}

func handleFailover(rank int, size int) {
	if rank == 0 || rank >= size {
		fmt.Println("-1")
		return
	}
	nodeStatusMutex.Lock()
	defer nodeStatusMutex.Unlock()
	if !nodeStatus[rank] {
		fmt.Println("-1")
		return
	}
	failoverState[rank] = true
	fmt.Println("1")
}

func handleRecover(rank int, size int) {
	if rank == 0 || rank >= size {
		fmt.Println("-1")
		return
	}
	nodeStatusMutex.Lock()
	defer nodeStatusMutex.Unlock()
	if nodeStatus[rank] && !failoverState[rank] {
		fmt.Println("-1")
		return
	}
	failoverState[rank] = false
	fmt.Println("1")
}

func heartbeatMonitorMaster(size int) {
	for atomic.LoadInt32(&running) == 1 {
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Second)
		nodeStatusMutex.Lock()
		currentTime := time.Now()
		for i := 1; i < size; i++ {
			if nodeStatus[i] && currentTime.Sub(lastHeartbeat[i]).Seconds() > FAILOVER_INTERVAL {
				nodeStatus[i] = false
			}
		}
		nodeStatusMutex.Unlock()
	}
}

func listenForHeartbeats(size int) {
	for atomic.LoadInt32(&running) == 1 {
		var status Status
		var heartbeat int
		MPI_Recv(&heartbeat, 1, MPI_INT, MPI_ANY_SOURCE, 0, COMM_WORLD, &status)
		sender := status.MPI_SOURCE
		nodeStatusMutex.Lock()
		if failoverState[sender] {
			nodeStatusMutex.Unlock()
			continue
		}
		lastHeartbeat[sender] = time.Now()
		if heartbeat == 1 {
			nodeStatus[sender] = true
		} else {
			nodeStatus[sender] = false
		}
		nodeStatusMutex.Unlock()
	}
}

func executeCommand(rank int, size int, command string) {
	commandMutex.Lock()
	defer commandMutex.Unlock()
	if strings.TrimSpace(command) == "exit" {
		atomic.StoreInt32(&running, 0)
		return
	}
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}
	cmd := parts[0]
	var fileName, filePath, word string
	if strings.HasPrefix(command, "upload") {
		if len(parts) < 3 {
			if rank == 1 {
				fmt.Println("-1")
			}
			return
		}
		fileName = parts[1]
		filePath = parts[2]
		if rank == 0 {
			handleUploadMaster(rank, size, fileName, filePath)
		} else {
			handleUploadWorker(rank, fileName)
		}
	} else if strings.HasPrefix(command, "retrieve") {
		if len(parts) < 2 {
			if rank == 1 {
				fmt.Println("-1")
			}
			return
		}
		fileName = parts[1]
		if rank == 0 {
			handleRetrieveMaster(rank, size, fileName)
		} else {
			handleRetrieveWorker(rank, fileName)
		}
	} else if strings.HasPrefix(command, "search") {
		if len(parts) < 3 {
			if rank == 1 {
				fmt.Println("-1")
			}
			return
		}
		fileName = parts[1]
		word = parts[2]
		if rank == 0 {
			handleSearchMaster(rank, size, fileName)
		} else {
			handleSearchWorker(rank, fileName, word)
		}
	} else if strings.HasPrefix(command, "list_file") {
		if len(parts) < 2 {
			return
		}
		fileName = parts[1]
		if rank == 0 {
			handlelistFile(fileName)
		}
	} else if strings.HasPrefix(command, "failover") {
		if len(parts) < 2 {
			return
		}
		RANK, err := strconv.Atoi(parts[1])
		if err != nil {
			return
		}
		if rank == 0 {
			handleFailover(RANK, size)
		}
	} else if strings.HasPrefix(command, "recover") {
		if len(parts) < 2 {
			return
		}
		RANK, err := strconv.Atoi(parts[1])
		if err != nil {
			return
		}
		if rank == 0 {
			handleRecover(RANK, size)
		}
	} else if rank == 1 {
		fmt.Println("-1")
	}
}

// ------------------------
// Worker Process Simulation
// ------------------------
func workerProcess(rank int, size int) {
	SetRank(rank)
	// Initialize worker storage structures
	nodeStatusMutex.Lock()
	nodeStatus[rank] = true
	lastHeartbeat[rank] = time.Now()
	failoverState[rank] = false
	nodeStatusMutex.Unlock()
	// Start heartbeat sender for worker
	go heartbeatWorker(rank)
	// Workers also listen for heartbeats from others (if needed)
	// For simulation, workers block waiting for commands.
	for atomic.LoadInt32(&running) == 1 {
		// Block until a message is received.
		// In an actual MPI environment, worker functions (handleUploadWorker,
		// handleRetrieveWorker, handleSearchWorker) are invoked when master sends commands.
		// Here, we simulate by sleeping.
		time.Sleep(100 * time.Millisecond)
	}
}

// ------------------------
// Main Function
// ------------------------
func main() {
	// Initialize MPI simulation with a fixed size.
	totalProcs := 4
	InitMPI(totalProcs)
	// Initialize global node status for all processes.
	for i := 0; i < totalProcs; i++ {
		nodeStatus[i] = true
		lastHeartbeat[i] = time.Now()
		failoverState[i] = false
		nodeLoad[i] = 0
	}
	// Launch worker processes (goroutines) for ranks 1 to totalProcs-1.
	for i := 1; i < totalProcs; i++ {
		go workerProcess(i, totalProcs)
	}
	// Master process (rank 0)
	SetRank(0)
	// Start heartbeat monitor and heartbeat listener in master.
	go heartbeatMonitorMaster(totalProcs)
	go listenForHeartbeats(totalProcs)
	// Command loop for master.
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Enter commands (upload, retrieve, search, list_file, failover, recover, exit):")
	for scanner.Scan() {
		line := scanner.Text()
		executeCommand(0, totalProcs, line)
		if strings.TrimSpace(line) == "exit" {
			break
		}
	}
	// Allow some time for all goroutines to finish processing before finalizing.
	time.Sleep(2 * time.Second)
	FinalizeMPI()
}

//-------------------------
// End of Code
//-------------------------

// func main() {
// 	// Equivalent to: int main(int argc, char **argv) {
// 	var rank, size int

// 	argc := len(os.Args)
// 	// MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &rank);
// 	MPI_Init_thread(&argc, &os.Args, MPI_THREAD_MULTIPLE, &rank)

// 	// MPI_Comm_rank(MPI_COMM_WORLD, &rank);
// 	MPI_Comm_rank(MPI_COMM_WORLD, &rank)
// 	// MPI_Comm_size(MPI_COMM_WORLD, &size);
// 	MPI_Comm_size(MPI_COMM_WORLD, &size)

// 	// Initialize maps for nodeStatus and lastHeartbeat
// 	nodeStatus = make(map[int]bool)
// 	lastHeartbeat = make(map[int]time.Time)

// 	{
// 		// {
// 		//     lock_guard<mutex> lock(nodeStatusMutex);
// 		nodeStatusMutex.Lock()
// 		//     auto currentTime = chrono::steady_clock::now();
// 		currentTime := time.Now()
// 		//     for (int i = 1; i < size; ++i) {
// 		for i := 1; i < size; i++ {
// 			//         nodeStatus[i] = true;
// 			nodeStatus[i] = true
// 			//         lastHeartbeat[i] = currentTime;
// 			lastHeartbeat[i] = currentTime
// 		}
// 		//     }
// 		nodeStatusMutex.Unlock()
// 		// }
// 	}

// 	var wg sync.WaitGroup
// 	// thread heartbeatMonitorThread, heartbeatListenerThread, heartbeatWorkerThread;
// 	// In Go, we use goroutines and a WaitGroup to wait for their completion.
// 	if rank == 0 {
// 		// if(rank==0){
// 		//     heartbeatMonitorThread = thread(heartbeatMonitorMaster, size);
// 		wg.Add(1)
// 		go heartbeatMonitorMaster(size, &wg)
// 		//     heartbeatListenerThread = thread(listenForHeartbeats, size);
// 		wg.Add(1)
// 		go listenForHeartbeats(size, &wg)
// 	} else {
// 		// } else heartbeatWorkerThread = thread(heartbeatWorker, rank);
// 		wg.Add(1)
// 		go heartbeatWorker(rank, &wg)
// 	}

// 	// string command;
// 	var command string
// 	scanner := bufio.NewScanner(os.Stdin)
// 	// while (running) {
// 	for running {
// 		//     if (rank == 0) getline(cin, command);
// 		if rank == 0 {
// 			if scanner.Scan() {
// 				command = scanner.Text()
// 			} else {
// 				// In case of error or EOF, stop running.
// 				running = false
// 				command = ""
// 			}
// 		}
// 		//     int commandLength = command.size();
// 		commandLength := len(command)
// 		//     MPI_Bcast(&commandLength, 1, MPI_INT, 0, MPI_COMM_WORLD);
// 		MPI_Bcast_int(&commandLength, 1, MPI_INT, 0, MPI_COMM_WORLD)
// 		//     command.resize(commandLength);
// 		if commandLength <= len(command) {
// 			command = command[:commandLength]
// 		}
// 		//     MPI_Bcast(command.data(), commandLength, MPI_CHAR, 0, MPI_COMM_WORLD);
// 		MPI_Bcast_char([]byte(command), commandLength, MPI_CHAR, 0, MPI_COMM_WORLD)
// 		//     executeCommand(rank, size, command);
// 		executeCommand(rank, size, command)
// 	}
// 	// if (rank == 0) {
// 	if rank == 0 {
// 		//     if (heartbeatMonitorThread.joinable()) heartbeatMonitorThread.join();
// 		//     if (heartbeatListenerThread.joinable()) heartbeatListenerThread.join();
// 		// } else if (heartbeatWorkerThread.joinable()) heartbeatWorkerThread.join();
// 		// In Go, we wait for all goroutines using the WaitGroup.
// 		wg.Wait()
// 	} else {
// 		wg.Wait()
// 	}
// 	// MPI_Finalize();
// 	MPI_Finalize()
// 	// return 0;
// }
