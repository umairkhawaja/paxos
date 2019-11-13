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
	LOGF.Println(pn.myAddr, " is Proposing: ", args.N, " For key: ", args.Key, " with value: ", args.V)
	result := make(chan paxosrpc.ProposeReply)
	startOver := make(chan bool)
	prepareResponses := make(chan paxosrpc.PrepareReply, len(pn.nodes))
	acceptResponses := make(chan paxosrpc.AcceptReply, len(pn.nodes))
	// The actual work is done in this go routine
	go func() {
		promises := make([]paxosrpc.PrepareReply, len(pn.nodes))

		majorityOn := int(len(pn.nodes) / 2)
		prepareArgs := paxosrpc.PrepareArgs{}
		prepareArgs.Key = args.Key
		prepareArgs.N = args.N
		prepareArgs.RequesterId = pn.myID
		// 2) Broadcast Prepare to all paxos nodes
		for k, client := range pn.nodes {
			go func(c *rpc.Client, k string) {
				reply := paxosrpc.PrepareReply{}
				c.Call("PaxosNode.RecvPrepare", &prepareArgs, &reply)
				// LOGF.Println("RECEIVED PREPARE FROM", k)
				prepareResponses <- reply
				return
			}(client, k)
		}
		// 4) Wait for MAJORITY responses
		func() {
			for {
				select {
				case reply := <-prepareResponses:
					LOGF.Println("Got a Promise: ", reply.N_a, reply.V_a, reply.Status)
					promises = append(promises, reply)
					if len(promises) > majorityOn { // Do not wait for all replies.
						return
					}
				default:
				}
			}
		}()
		// 4) Process the replies and generate Accept Message
		noAcceptedValues := true
		maxAcceptedProposal := args.N
		var maxAcceptedValue interface{}
		maxAcceptedValue = args.V
		for _, promise := range promises {
			if promise.N_a != -1 && promise.V_a != nil {
				noAcceptedValues = false
				if promise.N_a > maxAcceptedProposal {
					maxAcceptedProposal = promise.N_a
					maxAcceptedValue = promise.V_a
				}
			}
		}
		acceptMessage := paxosrpc.AcceptArgs{}
		if noAcceptedValues {
			acceptMessage.N = args.N
			acceptMessage.V = args.V
			acceptMessage.Key = args.Key
		} else {
			acceptMessage.N = maxAcceptedProposal
			acceptMessage.V = maxAcceptedValue
			acceptMessage.Key = args.Key
			args.N = maxAcceptedProposal
			args.V = maxAcceptedValue
		}
		acceptMessage.RequesterId = pn.myID
		// 5) Broadcast Accept message
		responses := make([]paxosrpc.AcceptReply, len(pn.nodes))
		for _, client := range pn.nodes {
			go func(c *rpc.Client) {
				reply := paxosrpc.AcceptReply{}
				msg := acceptMessage
				c.Call("PaxosNode.RecvAccept", &msg, &reply)
				// LOGF.Println("RECEIVED ACCEPT")
				acceptResponses <- reply
				return
			}(client)
		}
		// 5) Wait for MAJORITY responses
		func() {
			for {
				select {
				case reply := <-acceptResponses:
					// LOGF.Println("Got response to Accept: ", reply.N_a, reply.V_a, reply.Status)
					responses = append(responses, reply)
					if len(responses) > majorityOn { // Do not wait for all replies.
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
		} else {
			// 6b) Commit and update (key,value) pair
			committedMessage := acceptMessage
			for k, client := range pn.nodes {
				commArgs := paxosrpc.CommitArgs{}
				commArgs.Key = committedMessage.Key
				commArgs.V = committedMessage.V
				LOGF.Println("Sending Commit for: ", commArgs.Key, commArgs.V, " to : ", k)
				if commArgs.Key != "" {
					commArgs.RequesterId = committedMessage.RequesterId
					commitResp := paxosrpc.CommitReply{}
					client.Call("PaxosNode.RecvCommit", &commArgs, &commitResp)
				}
				// LOGF.Println("RECEIVED COMMIT")
			}
			commitedData := paxosrpc.ProposeReply{}
			commitedData.V = committedMessage.V
			result <- commitedData
		}

	}()
	// Construct to make use of timeouts
	select {
	case <-time.After(PROPOSE_TIMEOUT):
		return errors.New("PROPOSE TIMED OUT")
	case res := <-result:
		reply.V = res.V
		return nil
	case <-startOver:
		// 6a)  If paxos rejected value, then start over
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
	// LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	proposedNumber := args.N
	key := args.Key
	pn.minProposalMutex.Lock()
	pn.proposalsMutex.Lock()
	pn.valuesMutex.Lock()
	minProposal, ok := pn.minProposalNumbers[key]
	if !ok {
		pn.minProposalNumbers[key] = proposedNumber
		reply.N_a = -1
		reply.V_a = nil
		reply.Status = paxosrpc.OK
	} else if proposedNumber > minProposal {
		pn.minProposalNumbers[key] = proposedNumber
		acceptedProposal, aok := pn.acceptedProposals[key]
		if aok {
			reply.N_a = acceptedProposal
			reply.V_a = pn.acceptedValues[key]
			reply.Status = paxosrpc.OK
		} else {
			reply.N_a = -1
			reply.V_a = nil
			reply.Status = paxosrpc.OK
		}
	} else {
		reply.N_a = pn.acceptedProposals[key]
		reply.V_a = pn.acceptedValues[key]
		reply.Status = paxosrpc.Reject
	}
	pn.minProposalMutex.Unlock()
	pn.proposalsMutex.Unlock()
	pn.valuesMutex.Unlock()
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
	// LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	proposalNumber := args.N
	key := args.Key
	value := args.V
	pn.minProposalMutex.Lock()
	pn.proposalsMutex.Lock()
	pn.valuesMutex.Lock()
	minProposal, ok := pn.minProposalNumbers[key]
	if !ok {
		pn.acceptedProposals[key] = proposalNumber
		pn.minProposalNumbers[key] = proposalNumber
		pn.acceptedValues[key] = value
		reply.Status = paxosrpc.OK
	} else if proposalNumber >= minProposal {
		pn.acceptedProposals[key] = proposalNumber
		pn.minProposalNumbers[key] = proposalNumber
		pn.acceptedValues[key] = value
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}
	pn.proposalsMutex.Unlock()
	pn.valuesMutex.Unlock()
	pn.minProposalMutex.Unlock()
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
	LOGF := log.New(file, "", log.Lshortfile|log.Lmicroseconds)

	key := args.Key
	value := args.V
	LOGF.Println("Committing: ", key, value)
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
