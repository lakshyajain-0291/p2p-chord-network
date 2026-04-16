#include "NetworkController.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include "ClientNode.h"

using namespace omnetpp;

Define_Module(NetworkController);

void NetworkController::initialize()
{
    topoFilePath = par("topoFile").stdstringValue();
    outputFilePath = par("outputFile").stdstringValue();
    minDelay = par("minDelay").doubleValue();
    maxDelay = par("maxDelay").doubleValue();

    truncateOutputFile();
    ClientNode::resetRegistry();

    totalNodes = loadNodeCountFromTopo(topoFilePath);
    if (totalNodes <= 0) {
        throw cRuntimeError("Topology file '%s' must define N > 0", topoFilePath.c_str());
    }

    logLine("[INIT] Loaded topology file with N = " + std::to_string(totalNodes) + " nodes");

    createClientNodes();
    connectNodeGates();
    buildRingTopology();
    buildChordFingerTables();

    scheduleAt(simTime() + 0.1, new cMessage("start-task-phase"));
}

void NetworkController::handleMessage(cMessage *msg)
{
    if (strcmp(msg->getName(), "start-task-phase") == 0) {
        startTaskPhase();
    }

    delete msg;
}

int NetworkController::loadNodeCountFromTopo(const std::string& path)
{
    std::ifstream in(path);
    if (!in.is_open()) {
        throw cRuntimeError("Cannot open topology file '%s'", path.c_str());
    }

    int n = 0;
    in >> n;
    return n;
}

void NetworkController::truncateOutputFile() const
{
    std::ofstream out(outputFilePath, std::ios::trunc);
    out << "# OMNeT++ Assignment 2 detailed execution log\n";
}

void NetworkController::logLine(const std::string& line) const
{
    EV << line << "\n";
    std::ofstream out(outputFilePath, std::ios::app);
    out << simTime() << " " << line << "\n";
}

void NetworkController::createClientNodes()
{
    nodes.clear();
    nodes.reserve(totalNodes);

    cModuleType *clientType = cModuleType::get("ClientNode");
    if (clientType == nullptr) {
        throw cRuntimeError("ClientNode type not found");
    }

    for (int i = 0; i < totalNodes; ++i) {
        std::string name = "client-" + std::to_string(i);

        cModule *module = clientType->create(name.c_str(), this);
        module->par("nodeId").setIntValue(i);
        module->par("totalNodes").setIntValue(totalNodes);
        module->par("outputFile").setStringValue(outputFilePath.c_str());
        module->par("minDelay").setDoubleValue(minDelay);
        module->par("maxDelay").setDoubleValue(maxDelay);

        module->finalizeParameters();
        module->buildInside();
        module->scheduleStart(simTime());
        module->callInitialize();

        ClientNode *node = check_and_cast<ClientNode *>(module);
        nodes.push_back(node);
    }
}

void NetworkController::connectNodeGates()
{
    for (auto *node : nodes) {
        cModule *module = check_and_cast<cModule *>(node);
        module->setGateSize("out", totalNodes);
        module->setGateSize("in", totalNodes);
    }

    for (int from = 0; from < totalNodes; ++from) {
        for (int to = 0; to < totalNodes; ++to) {
            cGate *src = nodes[from]->gate("out", to);
            cGate *dst = nodes[to]->gate("in", from);
            if (!src->isConnected()) {
                src->connectTo(dst);
            }
        }
    }
}

void NetworkController::buildRingTopology()
{
    for (int i = 0; i < totalNodes; ++i) {
        int succ = (i + 1) % totalNodes;
        int pred = (i - 1 + totalNodes) % totalNodes;
        nodes[i]->setRingNeighbors(succ, pred);

        logLine("[RING] Node " + std::to_string(i)
                + " connected to successor " + std::to_string(succ)
                + " and predecessor " + std::to_string(pred));
    }

    logLine("[RING COMPLETE] Ring topology established");
}

void NetworkController::buildChordFingerTables()
{
    logLine("[CHORD] BEFORE optimization: max hops = O(N)");

    for (auto *node : nodes) {
        node->buildFingerTable();
    }

    logLine("[CHORD] AFTER optimization: expected hops ~= O(log N)");
}

void NetworkController::startTaskPhase()
{
    initiatorId = intuniform(0, totalNodes - 1);
    nodes[initiatorId]->markInitiator(true);

    logLine("[TASK INIT] Node " + std::to_string(initiatorId) + " selected as initiator");

    nodes[initiatorId]->generateAndDispatchSubtasks();
}
