package paxos

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"paxosapp/rpc/paxosrpc"
	"sync"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

const (
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)
)

var name string
var file *os.File

type paxosNode struct {
	// TODO: implement this!
	nodes              map[string]*rpc.Client
	myID               int
	myAddr             string
	minProposalNumbers map[string]int
	maxRoundNumber     map[string]int
	acceptedProposals  map[string]int
	acceptedValues     map[string]interface{}
	database           map[string]interface{}
	dbMutex            *sync.Mutex
	minProposalMutex   *sync.Mutex
	valuesMutex        *sync.Mutex
	proposalsMutex     *sync.Mutex
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	// node := &paxosNode{}
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	name = myHostPort + " log.txt"
	file, _ = os.OpenFile(name, flag, perm)
	node := new(paxosNode)
	node.nodes = make(map[string]*rpc.Client)
	node.minProposalNumbers = make(map[string]int)
	node.acceptedProposals = make(map[string]int)
	node.acceptedValues = make(map[string]interface{})
	node.maxRoundNumber = make(map[string]int)
	node.database = make(map[string]interface{})
	node.myID = srvId
	node.myAddr = hostMap[srvId]
	node.dbMutex = new(sync.Mutex)
	node.minProposalMutex = new(sync.Mutex)
	node.valuesMutex = new(sync.Mutex)
	node.proposalsMutex = new(sync.Mutex)

	prpc := paxosrpc.Wrap(node)
	rpc.Register(prpc)
	rpc.HandleHTTP()

	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		LOGF.Println(err)
		return nil, err
	}
	go http.Serve(ln, nil)

	for _, v := range hostMap {
		client, dialErr := rpc.DialHTTP("tcp", v)
		node.nodes[v] = client

		for i := numRetries; i >= 0; i-- {
			if dialErr == nil {
				break
			}
			time.Sleep(1 * time.Second)
			client, dialErr = rpc.DialHTTP("tcp", v)
			node.nodes[v] = client

		}
		if dialErr != nil {
			LOGF.Println(dialErr)
			return nil, dialErr
		}
	}
	// LOGF.Println("NumNodes:", len(node.nodes))
	return node, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	pn.maxRoundNumber[args.Key]++
	reply.N = mergeNumbers(pn.maxRoundNumber[args.Key], pn.myID)
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	LOGF.Println("My address: ", pn.myAddr)
	result := make(chan *paxosrpc.ProposeReply)
	startOver := make(chan bool)
	prepareResponses := make(chan *paxosrpc.PrepareReply, len(pn.nodes))
	acceptResponses := make(chan *paxosrpc.AcceptReply, len(pn.nodes))
	// The actual work is done in this go routine
	go func() {
		promises := make([]*paxosrpc.PrepareReply, len(pn.nodes))

		majorityOn := int(len(pn.nodes) / 2)
		prepareArgs := new(paxosrpc.PrepareArgs)
		prepareArgs.Key = args.Key
		prepareArgs.N = args.N
		prepareArgs.RequesterId = pn.myID
		// 2) Broadcast Prepare to all paxos nodes
		// fmt.Println("Sending Prepare to : ", len(pn.nodes))
		for k, client := range pn.nodes {
			go func(c *rpc.Client, k string) {
				reply := new(paxosrpc.PrepareReply)
				// reply := paxosrpc.PrepareReply{}
				err := c.Call("PaxosNode.RecvPrepare", prepareArgs, &reply)
				LOGF.Println(err)
				LOGF.Println("RECEIVED PREPARE FROM", k)
				prepareResponses <- reply
				return
			}(client, k)
		}
		// 4) Wait for MAJORITY responses
		func() {
			for {
				select {
				case reply := <-prepareResponses:
					promises = append(promises, reply)
					if len(promises) >= majorityOn { // Do not wait for all replies.
						return
					}
				default:
				}
			}
		}()
		// 4) Process the replies and generate Accept Message
		noAcceptedValues := true
		// acceptMessage := new(paxosrpc.AcceptArgs)
		acceptMessage := paxosrpc.AcceptArgs{}
		_ = noAcceptedValues
		_ = acceptMessage
		for _, promise := range promises {
			if promise.N_a != -1 && promise.V_a != nil {
				noAcceptedValues = false
				if acceptMessage.N == 0 && acceptMessage.V == nil {
					acceptMessage.N = promise.N_a
					acceptMessage.V = promise.V_a
				} else if promise.N_a > acceptMessage.N {
					acceptMessage.N = promise.N_a
					acceptMessage.V = promise.V_a
				}
			}
		}
		if noAcceptedValues {
			acceptMessage.N = args.N
			acceptMessage.V = args.V
			acceptMessage.Key = args.Key
			acceptMessage.RequesterId = pn.myID
		}
		// LOGF.Println("Accepted Message:", acceptMessage.N, acceptMessage.V)
		// 5) Broadcast Accept message
		responses := make([]*paxosrpc.AcceptReply, len(pn.nodes))
		for _, client := range pn.nodes {
			go func(c *rpc.Client) {
				reply := new(paxosrpc.AcceptReply)
				// msg := acceptMessage
				// reply := paxosrpc.AcceptReply{}
				err := c.Call("PaxosNode.RecvAccept", &acceptMessage, &reply)
				LOGF.Println(err)
				LOGF.Println("RECEIVED ACCEPT")
				acceptResponses <- reply
				return
			}(client)
		}
		// 5) Wait for MAJORITY responses
		func() {
			for {
				select {
				case reply := <-acceptResponses:
					responses = append(responses, reply)
					if len(responses) >= majorityOn { // Do not wait for all replies.
						return
					}
				default:
				}
			}
		}()
		// 6) Check if paxos accepted or rejected it
		anyRejections := false
		for _, response := range responses {
			if response.Status == paxosrpc.Reject {
				anyRejections = true
				break
			}
		}
		if anyRejections {
			// Start over with new proposal number
			startOver <- true
			// pn.nodes[pn.myID].Call("Propose")
		} else {
			// 6b) Commit and update (key,value) pair
			// pn.database[acceptMessage.Key] = acceptMessage.V
			for _, client := range pn.nodes {
				commitData := new(paxosrpc.CommitArgs)
				commitData.Key = acceptMessage.Key
				commitData.V = acceptMessage.V
				commitData.RequesterId = acceptMessage.RequesterId // CHECK THIS LATER
				commitResp := new(paxosrpc.CommitReply)
				client.Call("PaxosNode.RecvCommit", commitData, &commitResp)
				LOGF.Println("RECEIVED COMMIT")
			}
			commitedData := new(paxosrpc.ProposeReply)
			commitedData.V = acceptMessage.V
			result <- commitedData
		}

	}()
	// Construct to make use of timeouts
	select {
	case <-time.After(PROPOSE_TIMEOUT):
		return errors.New("PROPOSE TIMED OUT")
	case res := <-result:
		reply = res
		return nil
	case <-startOver:
		// 6a)  If paxos rejected value, then start over
		// pnArgs := new(paxosrpc.ProposalNumberArgs)
		// pnArgs.Key = args.Key
		// proposalNumber := new(paxosrpc.ProposalNumberReply)
		// pn.GetNextProposalNumber(pnArgs, proposalNumber)
		// args.N = proposalNumber.N
		// pn.nodes[pn.myID].Call("Propose", args, reply)
		reply.V = nil
		return nil
	}
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	key := args.Key
	pn.dbMutex.Lock()
	val, ok := pn.database[key]
	pn.dbMutex.Unlock()
	if ok {
		reply.V = val
		reply.Status = paxosrpc.KeyFound
	} else {
		reply.Status = paxosrpc.KeyNotFound
	}
	return nil
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	proposedNumber := args.N
	key := args.Key
	minProposal, ok := pn.minProposalNumbers[key]
	if !ok {
		pn.minProposalMutex.Lock()
		pn.minProposalNumbers[key] = proposedNumber
		pn.minProposalMutex.Unlock()
		pn.proposalsMutex.Lock()
		reply.N_a = -1
		pn.proposalsMutex.Unlock()

		pn.valuesMutex.Lock()
		reply.V_a = nil
		pn.valuesMutex.Unlock()

		reply.Status = paxosrpc.OK
		// LOGF.Println("SATUS: ", reply.Status)
		// return nil
	} else if proposedNumber > minProposal {
		pn.minProposalMutex.Lock()
		pn.minProposalNumbers[key] = proposedNumber
		pn.minProposalMutex.Unlock()
		pn.proposalsMutex.Lock()
		reply.N_a = pn.acceptedProposals[key]
		pn.proposalsMutex.Unlock()

		pn.valuesMutex.Lock()
		reply.V_a = pn.acceptedValues[key]
		pn.valuesMutex.Unlock()

		reply.Status = paxosrpc.OK
		// LOGF.Println("SATUS: ", reply.Status)
		// return nil
	} else {
		reply.N_a = -1
		reply.V_a = nil
		reply.Status = paxosrpc.Reject
		// return nil
	}
	LOGF.Println(pn.myAddr, ": Here")
	return nil
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	proposalNumber := args.N
	key := args.Key
	value := args.V
	pn.minProposalMutex.Lock()
	minProposal, ok := pn.minProposalNumbers[key]
	if !ok {
		pn.proposalsMutex.Lock()
		pn.valuesMutex.Lock()
		pn.acceptedProposals[key] = proposalNumber
		pn.minProposalNumbers[key] = proposalNumber
		pn.acceptedValues[key] = value
		reply.Status = paxosrpc.OK
		pn.proposalsMutex.Unlock()
		pn.valuesMutex.Unlock()
	} else if proposalNumber >= minProposal {
		pn.proposalsMutex.Lock()
		pn.valuesMutex.Lock()
		pn.acceptedProposals[key] = proposalNumber
		pn.minProposalNumbers[key] = proposalNumber
		pn.acceptedValues[key] = value
		LOGF.Println("Accepted: ", proposalNumber, value)
		reply.Status = paxosrpc.OK
		pn.proposalsMutex.Unlock()
		pn.valuesMutex.Unlock()
	} else {
		reply.Status = paxosrpc.Reject
	}
	pn.minProposalMutex.Unlock()
	// LOGF.Println("Accepted?:", reply.Status)
	return nil
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	key := args.Key
	value := args.V
	pn.dbMutex.Lock()
	pn.database[key] = value
	pn.dbMutex.Unlock()
	return nil
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	return errors.New("not implemented")
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	return errors.New("not implemented")
}

func mergeNumbers(rn, id int) int {
	return ((rn << 32) | (id & 0xffff))
	// merged, err := strconv.Atoi(strconv.Itoa(rn) + strconv.Itoa(id))
	// if err != nil {
	// panic(err)
	// }
	// return merged
}

func splitNumber(num int) (int, int) {
	roundNumber := num >> 32
	serverID := num & 0xffff
	return roundNumber, serverID
}
