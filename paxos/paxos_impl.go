package paxos

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"paxosapp/rpc/paxosrpc"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	// TODO: implement this!
	nodes              map[int]*rpc.Client
	myID               int
	minProposalNumbers map[string]int
	maxRoundNumber     map[string]int
	database           map[string]interface{}
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
	node := new(paxosNode)
	node.nodes = make(map[int]*rpc.Client)
	node.minProposalNumbers = make(map[string]int)
	node.maxRoundNumber = make(map[string]int)
	node.database = make(map[string]interface{})
	node.myID = srvId

	prpc := paxosrpc.Wrap(node)
	srv := rpc.NewServer()
	srv.Register(prpc)
	srv.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	ln, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	http.Serve(ln, nil)

	for k, v := range hostMap {
		var e error
		for i := numRetries; i >= 0; i-- {
			client, dialErr := rpc.DialHTTP("tcp", v)
			if dialErr == nil {
				node.nodes[k] = client
				e = nil
				break
			} else {
				e = dialErr
				time.Sleep(1)
			}
		}
		if e != nil {
			return nil, e
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

	result := make(chan *paxosrpc.ProposeReply)
	startOver := make(chan bool)
	// The actual work is done in this go routine
	go func() {
		promises := make([]*paxosrpc.PrepareReply, len(pn.nodes))

		majorityOn := int(len(pn.nodes) / 2)
		prepareArgs := new(paxosrpc.PrepareArgs)
		prepareArgs.Key = args.Key
		prepareArgs.N = args.N
		prepareArgs.RequesterId = pn.myID
		// 2) Broadcast Prepare to all paxos nodes
		prepareResponses := make(chan *paxosrpc.PrepareReply, len(pn.nodes))
		for _, client := range pn.nodes {
			go func(c *rpc.Client) {
				reply := new(paxosrpc.PrepareReply)
				c.Call("RecvPrepare", prepareArgs, reply)
				prepareResponses <- reply
				return
			}(client)
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
		acceptMessage := new(paxosrpc.AcceptArgs)
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
		// 5) Broadcast Accept message
		acceptResponses := make(chan *paxosrpc.AcceptReply, len(pn.nodes))
		responses := make([]*paxosrpc.AcceptReply, len(pn.nodes))
		for _, client := range pn.nodes {
			go func(c *rpc.Client) {
				reply := new(paxosrpc.AcceptReply)
				c.Call("RecvAccept", acceptMessage, reply)
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
				client.Call("RecvCommit", commitData, commitResp)
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
		break
	}
	// 6a)  If paxos rejected value, then start over
	pnArgs := new(paxosrpc.ProposalNumberArgs)
	pnArgs.Key = args.Key
	proposalNumber := new(paxosrpc.ProposalNumberReply)
	pn.GetNextProposalNumber(pnArgs, proposalNumber)
	args.N = proposalNumber.N
	pn.nodes[pn.myID].Call("Propose", args, reply)
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	return errors.New("not implemented")
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
	return errors.New("not implemented")
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
	return errors.New("not implemented")
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
	return errors.New("not implemented")
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
