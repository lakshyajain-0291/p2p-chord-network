#include "ClientNode.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <sstream>

using namespace omnetpp;

std::vector<ClientNode *> ClientNode::allNodes;
bool ClientNode::globalGossipStarted = false;

Define_Module(ClientNode);

void ClientNode::initialize()
{
    nodeId = par("nodeId").intValue();
    totalNodes = par("totalNodes").intValue();
    minDelay = par("minDelay").doubleValue();
    maxDelay = par("maxDelay").doubleValue();
    outputFilePath = par("outputFile").stdstringValue();

    if ((int)allNodes.size() < totalNodes) {
        allNodes.resize(totalNodes, nullptr);
    }
    allNodes[nodeId] = this;
}

void ClientNode::handleMessage(cMessage *msg)
{
    if (auto *subtask = dynamic_cast<SubtaskMessage *>(msg)) {
        routeSubtask(subtask);
        return;
    }

    if (auto *result = dynamic_cast<ResultMessage *>(msg)) {
        routeResult(result);
        return;
    }

    if (auto *gossip = dynamic_cast<GossipMessage *>(msg)) {
        processGossip(gossip);
        return;
    }

    if (dynamic_cast<StartGossipMessage *>(msg) != nullptr) {
        generateGossip();
        delete msg;
        return;
    }

    delete msg;
}

void ClientNode::setRingNeighbors(int succ, int pred)
{
    successor = succ;
    predecessor = pred;
}

void ClientNode::buildFingerTable()
{
    fingerTable.clear();

    logLine("[CHORD] Node " + std::to_string(nodeId) + " computing finger table...");

    std::set<int> uniqueTargets;
    for (int k = 0; (1 << k) < totalNodes; ++k) {
        int finger = (nodeId + (1 << k)) % totalNodes;
        if (finger == nodeId) {
            continue;
        }
        if (uniqueTargets.insert(finger).second) {
            fingerTable.push_back(finger);
            logLine("[CHORD] Node " + std::to_string(nodeId) + " adds link to node "
                    + std::to_string(finger) + " (2^" + std::to_string(k) + " jump)");
        }
    }

    std::ostringstream oss;
    oss << "[CHORD COMPLETE] Node " << nodeId << " finger table: [";
    for (size_t i = 0; i < fingerTable.size(); ++i) {
        oss << fingerTable[i];
        if (i + 1 < fingerTable.size()) {
            oss << ", ";
        }
    }
    oss << "]";
    logLine(oss.str());
}

void ClientNode::markInitiator(bool value)
{
    isInitiator = value;
}

void ClientNode::generateAndDispatchSubtasks()
{
    Enter_Method("generateAndDispatchSubtasks()");

    if (!isInitiator) {
        return;
    }

    int subtasks = totalNodes + intuniform(1, totalNodes);
    int minElements = 2 * subtasks;
    int totalElements = intuniform(minElements, minElements + 5 * totalNodes);

    std::vector<int> data(totalElements);
    for (int i = 0; i < totalElements; ++i) {
        data[i] = intuniform(0, 999);
    }

    expectedSubtasks = subtasks;
    collectedResults.clear();

    logLine("[TASK SPLIT] Total elements = " + std::to_string(totalElements)
            + ", subtasks = " + std::to_string(subtasks));

    int baseSize = totalElements / subtasks;
    int extra = totalElements % subtasks;
    int cursor = 0;

    for (int i = 0; i < subtasks; ++i) {
        int sliceSize = baseSize + (i < extra ? 1 : 0);
        int target = i % totalNodes;

        logLine("[TASK SPLIT] Subtask " + std::to_string(i)
                + " assigned to client " + std::to_string(target));

        auto *msg = new SubtaskMessage(("subtask-" + std::to_string(i)).c_str());
        msg->setSubtaskId(i);
        msg->setInitiatorId(nodeId);
        msg->setTargetId(target);
        msg->setValuesArraySize(sliceSize);

        for (int idx = 0; idx < sliceSize; ++idx) {
            msg->setValues(idx, data[cursor + idx]);
        }
        cursor += sliceSize;

        routeSubtask(msg);
    }
}

void ClientNode::scheduleGossipStart()
{
    Enter_Method_Silent("scheduleGossipStart()");

    scheduleAt(simTime() + uniform(0.01, 0.15), new StartGossipMessage("start-gossip"));
}

ClientNode *ClientNode::getNodeById(int id)
{
    if (id < 0 || id >= (int)allNodes.size()) {
        return nullptr;
    }
    return allNodes[id];
}

void ClientNode::resetRegistry()
{
    allNodes.clear();
    globalGossipStarted = false;
}

int ClientNode::getRegistrySize()
{
    return (int)allNodes.size();
}

void ClientNode::logLine(const std::string& line) const
{
    EV << line << "\n";

    std::ofstream out(outputFilePath, std::ios::app);
    out << simTime() << " " << line << "\n";
}

void ClientNode::sendToNode(cMessage *msg, int nextNodeId)
{
    if (nextNodeId < 0 || nextNodeId >= totalNodes) {
        throw cRuntimeError("Invalid next node id %d", nextNodeId);
    }

    simtime_t latency = uniform(minDelay, maxDelay);
    sendDelayed(msg, latency, "out", nextNodeId);
}

int ClientNode::clockwiseDistance(int from, int to) const
{
    return (to - from + totalNodes) % totalNodes;
}

int ClientNode::findClosestPrecedingNode(int targetId) const
{
    for (int finger : fingerTable) {
        if (finger == targetId) {
            return finger;
        }
    }

    int distanceToTarget = clockwiseDistance(nodeId, targetId);
    if (distanceToTarget == 0) {
        return nodeId;
    }

    int bestNode = successor;
    int bestAdvance = clockwiseDistance(nodeId, successor);

    for (int finger : fingerTable) {
        int advance = clockwiseDistance(nodeId, finger);
        if (advance > 0 && advance < distanceToTarget && advance > bestAdvance) {
            bestAdvance = advance;
            bestNode = finger;
        }
    }

    return bestNode;
}

void ClientNode::routeSubtask(SubtaskMessage *msg)
{
    int target = msg->getTargetId();

    if (target == nodeId) {
        logLine("[DELIVERED] Subtask " + std::to_string(msg->getSubtaskId())
                + " reached Node " + std::to_string(nodeId));
        executeSubtask(msg);
        return;
    }

    int next = findClosestPrecedingNode(target);

    int fingerIndex = -1;
    for (size_t i = 0; i < fingerTable.size(); ++i) {
        if (fingerTable[i] == next) {
            fingerIndex = (int)i;
            break;
        }
    }

    if (fingerIndex >= 0) {
        logLine("[ROUTE DECISION] Using finger table entry " + std::to_string(fingerIndex));
    }
    else {
        logLine("[ROUTE DECISION] Using ring successor fallback");
    }

    logLine("[ROUTE] Subtask " + std::to_string(msg->getSubtaskId())
            + " at Node " + std::to_string(nodeId)
            + " -> forwarding to Node " + std::to_string(next)
            + " (target = " + std::to_string(target) + ")");

    sendToNode(msg, next);
}

void ClientNode::executeSubtask(SubtaskMessage *msg)
{
    int size = msg->getValuesArraySize();
    if (size <= 0) {
        throw cRuntimeError("Subtask %d has empty payload", msg->getSubtaskId());
    }

    logLine("[EXECUTE] Node " + std::to_string(nodeId)
            + " processing subtask " + std::to_string(msg->getSubtaskId()));

    int localMax = std::numeric_limits<int>::min();
    for (int i = 0; i < size; ++i) {
        localMax = std::max(localMax, msg->getValues(i));
    }

    logLine("[RESULT] Subtask " + std::to_string(msg->getSubtaskId())
            + " result = " + std::to_string(localMax));

    auto *result = new ResultMessage(("result-" + std::to_string(msg->getSubtaskId())).c_str());
    result->setSubtaskId(msg->getSubtaskId());
    result->setInitiatorId(msg->getInitiatorId());
    result->setSourceNodeId(nodeId);
    result->setResultValue(localMax);

    delete msg;

    if (nodeId == result->getInitiatorId()) {
        collectResult(result);
        return;
    }

    logLine("[RETURN] Subtask " + std::to_string(result->getSubtaskId()) + " result traveling back");
    routeResult(result);
}

void ClientNode::routeResult(ResultMessage *msg)
{
    int initiator = msg->getInitiatorId();

    if (initiator == nodeId) {
        collectResult(msg);
        return;
    }

    int next = findClosestPrecedingNode(initiator);

    logLine("[ROUTE BACK] Node " + std::to_string(nodeId)
            + " -> Node " + std::to_string(next));

    sendToNode(msg, next);
}

void ClientNode::collectResult(ResultMessage *msg)
{
    if (!isInitiator) {
        delete msg;
        return;
    }

    collectedResults[msg->getSubtaskId()] = msg->getResultValue();
    logLine("[COLLECT] Received result for subtask " + std::to_string(msg->getSubtaskId()));

    delete msg;

    if ((int)collectedResults.size() == expectedSubtasks) {
        int globalMax = std::numeric_limits<int>::min();
        for (const auto& kv : collectedResults) {
            globalMax = std::max(globalMax, kv.second);
        }

        logLine("[FINAL RESULT] Computed global max = " + std::to_string(globalMax));

        if (!globalGossipStarted) {
            globalGossipStarted = true;
            for (int i = 0; i < totalNodes; ++i) {
                ClientNode *node = getNodeById(i);
                if (node != nullptr) {
                    node->scheduleGossipStart();
                }
            }
        }
    }
}

void ClientNode::generateGossip()
{
    if (gossipGenerated) {
        return;
    }

    gossipGenerated = true;
    gossipMessageLog.insert(nodeId);

    std::ostringstream payload;
    payload << simTime() << ":" << nodeId << ":client-" << nodeId;

    logLine("[GOSSIP GENERATED] Node " + std::to_string(nodeId)
            + " created message " + payload.str());

    if (totalNodes == 1) {
        checkTermination();
        return;
    }

    auto *gossip = new GossipMessage(("gossip-" + std::to_string(nodeId)).c_str());
    gossip->setOriginNodeId(nodeId);
    gossip->setSenderNodeId(nodeId);
    gossip->setContent(payload.str().c_str());

    forwardGossipToAdjacentPeers(gossip, nodeId);

    checkTermination();
}

void ClientNode::processGossip(GossipMessage *msg)
{
    int origin = msg->getOriginNodeId();
    int sender = msg->getSenderNodeId();

    if (gossipMessageLog.count(origin) > 0) {
        logLine("[GOSSIP SKIP] Duplicate message ignored");
        delete msg;
        checkTermination();
        return;
    }

    gossipMessageLog.insert(origin);
    logLine("[GOSSIP RECEIVED] Node " + std::to_string(nodeId)
            + " received message from " + std::to_string(sender));

    forwardGossipToAdjacentPeers(msg, sender);

    checkTermination();
}

void ClientNode::forwardGossipToAdjacentPeers(GossipMessage *msg, int senderNodeId)
{
    std::set<int> peers;
    if (successor >= 0 && successor != nodeId && successor != senderNodeId) {
        peers.insert(successor);
    }
    if (predecessor >= 0 && predecessor != nodeId && predecessor != senderNodeId) {
        peers.insert(predecessor);
    }

    if (peers.empty()) {
        delete msg;
        return;
    }

    bool sentOriginal = false;
    for (int peerId : peers) {
        GossipMessage *outMsg = nullptr;
        if (!sentOriginal) {
            outMsg = msg;
            sentOriginal = true;
        }
        else {
            outMsg = check_and_cast<GossipMessage *>(msg->dup());
        }

        outMsg->setSenderNodeId(nodeId);
        logLine("[GOSSIP FORWARD] Node " + std::to_string(nodeId)
                + " -> Node " + std::to_string(peerId));
        sendToNode(outMsg, peerId);
    }
}

void ClientNode::checkTermination()
{
    if (!terminationLogged && (int)gossipMessageLog.size() == totalNodes) {
        terminationLogged = true;
        logLine("[TERMINATE] Node " + std::to_string(nodeId)
                + " received all gossip messages. Shutting down.");
    }
}
