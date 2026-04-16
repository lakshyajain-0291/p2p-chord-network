#ifndef __ASSIGNMENT2_CLIENTNODE_H_
#define __ASSIGNMENT2_CLIENTNODE_H_

#include <map>
#include <set>
#include <string>
#include <vector>

#include <omnetpp.h>

#include "messages_m.h"

class ClientNode : public omnetpp::cSimpleModule
{
  private:
    int nodeId = -1;
    int totalNodes = 0;
    int successor = -1;
    int predecessor = -1;

    double minDelay = 0.01;
    double maxDelay = 0.20;
    std::string outputFilePath;

    bool isInitiator = false;
    bool gossipGenerated = false;
    bool terminationLogged = false;
    int expectedSubtasks = 0;

    std::vector<int> fingerTable;
    std::map<int, int> collectedResults;
    std::set<int> gossipMessageLog;

    static std::vector<ClientNode *> allNodes;
    static bool globalGossipStarted;

  protected:
    virtual void initialize() override;
    virtual void handleMessage(omnetpp::cMessage *msg) override;

  public:
    void setRingNeighbors(int succ, int pred);
    void buildFingerTable();
    void markInitiator(bool value);
    void generateAndDispatchSubtasks();
    void scheduleGossipStart();

    int getNodeId() const { return nodeId; }
    const std::vector<int>& getFingerTable() const { return fingerTable; }

    static ClientNode *getNodeById(int id);
    static void resetRegistry();
    static int getRegistrySize();

  private:
    void logLine(const std::string& line) const;
    void sendToNode(omnetpp::cMessage *msg, int nextNodeId);
    void forwardGossipToAdjacentPeers(GossipMessage *msg, int senderNodeId);

    int clockwiseDistance(int from, int to) const;
    int findClosestPrecedingNode(int targetId) const;

    void routeSubtask(SubtaskMessage *msg);
    void executeSubtask(SubtaskMessage *msg);

    void routeResult(ResultMessage *msg);
    void collectResult(ResultMessage *msg);

    void generateGossip();
    void processGossip(GossipMessage *msg);
    void checkTermination();
};

#endif
