message SubtaskMessage extends cMessage
{
    int subtaskId;
    int initiatorId;
    int targetId;
    int values[];
}

message ResultMessage extends cMessage
{
    int subtaskId;
    int initiatorId;
    int sourceNodeId;
    int resultValue;
}

message GossipMessage extends cMessage
{
    int originNodeId;
    int senderNodeId;
    string content;
}

message StartGossipMessage extends cMessage
{
}
