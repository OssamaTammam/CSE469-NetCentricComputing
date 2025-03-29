package asg3

import (
	"log"
	"sync"
)

// This struct keeps track of an incoming link status
// It contains the the number of tokens and has the node received a marker message on this incoming channel to close it
type LinkState struct {
	tokens int  // The number of tokens on the link
	marked bool // true if the marker has been received on this link
}

// This struct represents a snapshot of a node.
// It contains the state of the node at the time of the snapshot.
// It also contains the state of the node's links states (To be updated till the snapshot is completed).
type NodeSnapshot struct {
	id           int                   // The id of the global snapshot
	localState   int                   // Tokens at the time of the snapshot start
	linksState   map[string]*LinkState // key = link.src, value = link state
	markedLinks  int                   // Keep track of how many links are marked in a snapshot
	isCompleted  bool                  // true if the snapshot is completed
	snapshotLock sync.RWMutex
}

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	snapshots     map[int]*NodeSnapshot // key = snapshotId
	tokensLock    sync.RWMutex
	snapshotsLock sync.RWMutex
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		snapshots:     make(map[int]*NodeSnapshot),
		tokensLock:    sync.RWMutex{},
		snapshotsLock: sync.RWMutex{},
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

// Responsible for adding tokens on incoming channels for all active snapshots
func (node *Node) RecordTokens(src string, tokens int) {
	node.tokensLock.Lock()
	node.tokens += tokens
	node.tokensLock.Unlock()

	node.snapshotsLock.RLock()
	defer node.snapshotsLock.RUnlock()

	for _, snapshot := range node.snapshots {
		snapshot.snapshotLock.Lock()
		if snapshot.isCompleted {
			snapshot.snapshotLock.Unlock()
			continue
		}

		// Add the tokens on the incoming channel and on the node
		linkState := snapshot.linksState[src]
		linkState.tokens += tokens

		snapshot.snapshotLock.Unlock()
		log.Printf("Node: %v, added %v tokens to incoming channel from %v\n",
			node.id, tokens, src)
	}
}

// Mark snapshot as completed
func (node *Node) CompleteSnapshot(snapshot *NodeSnapshot) {
	snapshot.snapshotLock.Lock()
	snapshot.isCompleted = true
	snapshot.snapshotLock.Unlock()

	go node.sim.NotifyCompletedSnapshot(node.id, snapshot.id)
	log.Printf("Node: %v, marked snapshot %v as completed", node.id, snapshot.id)
}

func (node *Node) HandlePacket(src string, message Message) {
	// TODO: Write this method
	if message.isMarker {
		log.Printf("Node: %v, received marker message from %v\n", node.id, src)

		snapshotId := message.data
		node.snapshotsLock.RLock()
		snapshot, exists := node.snapshots[snapshotId]
		node.snapshotsLock.RUnlock()

		// If snapshot exists, we update the link state
		// If snapshot does not exist, we start a new snapshot
		if exists {
			snapshot.snapshotLock.Lock()
			linkState := snapshot.linksState[src]
			linkState.marked = true
			snapshot.markedLinks += 1
			snapshot.snapshotLock.Unlock()

			log.Printf("Node: %v, received marker message from %v, link state updated\n", node.id, src)

			// Check if snapshot is completed
			if snapshot.markedLinks == len(node.inboundLinks) {
				node.CompleteSnapshot(snapshot)
			}
		} else {
			go node.StartSnapshot(snapshotId)
		}
	} else {
		log.Printf("Node: %v, received token message from %v\n", node.id, src)
		node.RecordTokens(src, message.data)
	}
}

func (node *Node) StartSnapshot(snapshotId int) {
	// ToDo: Write this method
	// Start snapshot
	log.Printf("Node: %v, starting snapshot %v\n", node.id, snapshotId)

	// Read tokens safely
	node.tokensLock.RLock()
	tokens := node.tokens
	node.tokensLock.RUnlock()

	// Create a new snapshot
	snapshot := NodeSnapshot{
		id:         snapshotId,
		localState: tokens,
		linksState: make(map[string]*LinkState),
	}

	node.snapshotsLock.Lock()
	for src := range node.inboundLinks {
		snapshot.linksState[src] = &LinkState{}
	}
	node.snapshots[snapshotId] = &snapshot
	node.snapshotsLock.Unlock()

	// Send marker to  all outbound links
	message := Message{isMarker: true, data: snapshotId}
	node.SendToNeighbors(message)
	log.Printf("Node: %v, sent marker message to all outbound links\n", node.id)
}
