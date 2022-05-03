#!/usr/bin/env python3

import argparse, socket, time, json, select, struct, sys, math, os

BROADCAST = "FFFF"

class Replica:

    def __init__(self, port, id, others):
        self.port = port
        self.id = id
        self.others = others

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', 0))

        self.last = time.time()
        # all replicas start as follower
        self.thisRole = 'follower'
        self.term = 0
        self.votedFor = ''
        self.votes = 0
        self.leader = 'FFFF'
        self.log = [{'src': '', 'key': '', 'value': '', 'MID': '', 'term': 0}]
        self.database = {}
        # if true, print debug info
        self.DEBUG = False
        print("Replica %s starting up" % self.id, flush=True)
        hello = { "src": self.id, "dst": BROADCAST, "leader": BROADCAST, "type": "hello" }
        self.send(hello)
        print("Sent hello message: %s" % hello, flush=True)

    def send(self, message):
        self.socket.sendto(json.dumps(message).encode('utf-8'), ('localhost', self.port))

    def run(self):
        while True:
            clock = time.time()

            # if this replica is the leader, send heartbeat every 0.5 sec
            if clock - self.last > 0.5 and self.thisRole == 'leader':
                debug_helper(my_id + " sending heartbeat")
                send_append_entries(replica_ids)
                self.last = clock

            # if this replica is not the leader and have not heard back from the leader for a while, start election
            if clock - self.last > 2.8 and self.thisRole != 'leader':
                debug_helper("Leader timed out, starting election")
                start_election()
                self.last = clock

            data, addr = self.socket.recvfrom(65535)
            msg = json.loads(data.decode('utf-8'))
            if msg['type'] == 'get':
                handle_get(msg)

            elif msg['type'] == 'put':
                handle_put(msg)

            elif msg['type'] == 'RequestVote RPC':
                handle_request_vote(msg)

            elif msg['type'] == 'VoteTrue':
                handle_vote(msg)

            elif msg['type'] == 'VoteFalse':
                handle_rejected_vote(msg)

            elif msg['type'] == 'AppendEntries RPC':
                self.last = time.time()
                handle_append_entries(msg)
            print("Received message '%s'" % (msg,), True)

    # initialize a new election
    def start_election(self):

        # increments its current term and transitions to candidate state.
        self.term += 1
        self.thisRole = 'candidate'

        # It then votes for itself and issues RequestVote RPCs
        self.votedFor = my_id
        self.votes += 1

        request_vote_msg = {'src': my_id,
                            'dst': BROADCAST,
                            'leader': self.leader,
                            'type': 'RequestVote RPC',
                            'term': self.term,
                            'candidate_id': my_id,
                            }

        debug_helper(request_vote_msg)
        # broadcast the request vote rpc to all replicas
        self.send(request_vote_msg)

    # helper function dealing with request vote, following RequestVote RPC section of page 4
    def handle_request_vote(self,request_vote_msg):
        debug_helper("Received a request vote from " + request_vote_msg['src'])

        request_vote_reply = {'src': my_id,
                              'dst': request_vote_msg['src'],
                              'leader': self.leader,
                              'type': 'Null',
                              'term': self.term
                              }

        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        if self.votedFor == '':
            request_vote_reply['type'] = 'VoteTrue'
            self.votedFor = request_vote_msg['src']
            self.send(request_vote_reply)
            debug_helper(str(my_id) + " sent a vote to " + str(request_vote_reply['dst']))
        else:
            request_vote_reply['type'] = 'VoteFalse'
            self.send(request_vote_reply)
            debug_helper("Already voted for " + str(self.votedFor))

    def handle_get(self,get_msg):
        key = get_msg['key']

        # if I am the leader, reply to the get
        if self.thisRole == 'leader' and self.leader == my_id:

            get_reply = {"src": my_id,
                         "dst": get_msg['src'],
                         "leader": self.leader,
                         "type": "ok",
                         "MID": get_msg['MID'],
                         "value": ''}

            # ok with value if in
            if key in self.database.keys():
                value = self.database[key]
                get_reply['value'] = value
                self.send(get_reply)
            else:
                get_reply = {"src": my_id,
                             "dst": get_msg['src'],
                             "leader": self.leader,
                             "type": "fail",
                             "MID": get_msg['MID']}
                self.send(get_reply)
        else:
            get_redirect_msg = {"src": my_id,
                                "dst": get_msg['src'],
                                "leader": self.leader,
                                "type": "redirect",
                                "MID": get_msg['MID']}
            self.send(get_redirect_msg)

    def handle_put(self,put_msg):
        # if I am the leader
        if self.thisRole == 'leader' and self.leader == my_id:
            self.database[put_msg['key']] = put_msg['value']
            self.log.append({'src': put_msg['src'],
                        'key': put_msg['key'],
                        'value': put_msg['value'],
                        'MID': put_msg['MID'],
                        'term': self.term})

            append_entries_message = {"src": my_id, "dst": BROADCAST, "leader": self.leader, "type": "AppendEntries RPC",
                                      "MID": put_msg['MID'], "key": put_msg['key'], "value": put_msg['value']}

            i = 0
            while i < 2:
                self.send(append_entries_message)
                i += 1

            ok_put_message = {"src": my_id, "dst": put_msg['src'], "leader": self.leader, "type": "ok",
                              "MID": put_msg['MID']}
            self.send(ok_put_message)

        else:
            if self.leader == 'FFFF':
                start_election()
            else:
                # if i am not the leader, redirect
                redirect_put_msg = {"src": my_id,
                                    "dst": put_msg['src'],
                                    "leader": self.leader,
                                    "type": "redirect",
                                    "MID": put_msg['MID']}
                self.send(redirect_put_msg)
                debug_helper(
                    str(my_id) + " redirected a put request + current role and leader are " + str(self.leader) + self.thisRole)

    def handle_vote(self,vote_true_msg):
        self.votes += 1

        debug_helper(str(my_id) + " received a vote from " + vote_true_msg['src'])

        # if we have the most majority of votes
        if self.votes >= len(replica_ids) / 2:
            # set leader related info to my_id
            self.leader = my_id
            self.thisRole = 'leader'

            # reset global variables
            self.votes = 0
            send_append_entries(replica_ids)

    def handle_rejected_vote(self,rejection_msg):
        if self.thisRole == 'candidate' and rejection_msg['term'] >= self.term:
            self.term = rejection_msg['term']
            self.thisRole = 'follower'

            # reset election variable
            self.votes = 0
            self.votedFor = ''
            self.leader = rejection_msg['leader']

    def handle_append_entries(self,append_entries_rpc):
        if append_entries_rpc['key'] == '' or append_entries_rpc['value'] == '':
            # heartbeat
            self.votedFor = ''
            if self.leader == 'FFFF' or not self.leader == append_entries_rpc['leader']:
                self.leader = append_entries_rpc['leader']
                debug_helper(str(my_id) + " set leader to " + str(self.leader))
        else:
            self.database[append_entries_rpc['key']] = append_entries_rpc['value']

    def send_append_entries(self,destination):
        if self.leader == my_id and self.thisRole == 'leader':
            for replica_id in destination:
                append_entries_msg = {
                    'src': my_id,
                    'dst': replica_id,
                    'type': 'AppendEntries RPC',
                    'leader': self.leader,
                    'term': self.term,
                    "key": '', "value": ''
                }

                self.send(append_entries_msg)

                debug_helper(str(my_id) + " sent append entry RPC to " + str(replica_id))
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run a key-value store')
    parser.add_argument('port', type=int, help="Port number to communicate")
    parser.add_argument('id', type=str, help="ID of this replica")
    parser.add_argument('others', metavar='others', type=str, nargs='+', help="IDs of other replicas")
    args = parser.parse_args()
    replica = Replica(args.port, args.id, args.others)
    replica.run()