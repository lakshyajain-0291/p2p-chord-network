#ifndef __ASSIGNMENT2_NETWORKCONTROLLER_H_
#define __ASSIGNMENT2_NETWORKCONTROLLER_H_

#include <string>
#include <vector>

#include <omnetpp.h>

class ClientNode;

class NetworkController : public omnetpp::cSimpleModule
{
  private:
    int totalNodes = 0;
    std::string topoFilePath;
    std::string outputFilePath;
    double minDelay = 0.01;
    double maxDelay = 0.20;

    std::vector<ClientNode *> nodes;
    int initiatorId = -1;

  protected:
    virtual void initialize() override;
    virtual void handleMessage(omnetpp::cMessage *msg) override;

  private:
    int loadNodeCountFromTopo(const std::string& path);
    void truncateOutputFile() const;
    void logLine(const std::string& line) const;

    void createClientNodes();
    void connectNodeGates();
    void buildRingTopology();
    void buildChordFingerTables();
    void startTaskPhase();
};

#endif
