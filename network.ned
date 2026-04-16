simple ClientNode
{
    parameters:
        int nodeId @mutable = -1;
        int totalNodes @mutable = 0;
        string outputFile @mutable = default("outputfile.txt");
        double minDelay @mutable @unit(s) = default(0.01s);
        double maxDelay @mutable @unit(s) = default(0.20s);
        @class(ClientNode);
    gates:
        input in[];
        output out[];
}

simple NetworkController
{
    parameters:
        string topoFile = default("topo.txt");
        string outputFile = default("outputfile.txt");
        double minDelay @unit(s) = default(0.01s);
        double maxDelay @unit(s) = default(0.20s);
        @class(NetworkController);
}

network P2PNetwork
{
    parameters:
        string topoFile = default("topo.txt");
        string outputFile = default("outputfile.txt");
        double minDelay @unit(s) = default(0.01s);
        double maxDelay @unit(s) = default(0.20s);
    submodules:
        controller: NetworkController {
            parameters:
                topoFile = default(parent.topoFile);
                outputFile = default(parent.outputFile);
                minDelay = default(parent.minDelay);
                maxDelay = default(parent.maxDelay);
        }
}
