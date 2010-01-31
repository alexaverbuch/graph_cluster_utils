didic_neo4j is a Maven project, built in Eclipse.

It's a test implementation of the DiDiC [1] decentralized dynamic partitioning algorithm. 
The algorithm is implemented on a single, coloured Neo4j graph at present.
This is still in centralized mode and well and truly in "proof of concept" stage, but the algorithm is proven to work in large P2P systems.

To use:
	// Create NeoFromFile instance and assign DB location
	// * Refer to my graph_gen_utils github repo
	NeoFromFile neoCreator = new NeoFromFile("var/test-DiDiC");

	// To generate coloured/partitioned neo4j graph
	// * Assign input Chaco graph file & input Partitioning file 
	neoCreator1.generateNeo("graphs/test-DiDiC.graph",
			"partitionings/test-DiDiC.2.ptn");

	// Create DiDiCPartitioner, which implements the DiDiC algorithm
	DiDiCPartitioner didic = new DiDiCPartitioner(2, "var/test-DiDiC");

	// Magic happens here...
	didic.do_DiDiC(150);

To visualize the results for verification I recommend using NeoClipse, the Neo4j Eclipse plugin.
Screen captures from NeoClipse can be found in this repository in the /images folder.

[1] Joachim Gehweiler and Henning Meyerhenke. A Distributed Diffusive Heuristic for Clustering a Virtual P2P Supercomputer, University of Paderborn, 2010.

