package example;

import graph_cluster_algorithms.ClusterAlgDiDiC;
import graph_cluster_utils.Supervisor;
import graph_cluster_utils.SupervisorDiDiC;
import graph_gen_utils.NeoFromFile;

import java.io.FileNotFoundException;

public class ClusteringExample {
  
  // Debugging Related
  private static final int SNAPSHOT_PERIOD = 5;
  private static final int MAX_ITERATIONS = 150;
  
  public static void main(String[] args) {
    // do_didic_add20_16_base_random();
    // do_didic_add20_16_base_balanced();
    do_didic_add20_16_opt_random();
    do_didic_add20_16_opt_balanced();
  }
  
  private static void do_didic_add20_16_base_random() {
    int clusterCount = 16;
    
    String inputGraph = "add20";
    String inputPtn = "add20-IN-RAND";
    
    String databaseDir = String.format("var/%s-16-random", inputGraph);
    
    String graphDir = "graphs/";
    String ptnDir = "partitionings/";
    String metDir = "metrics/add20 16 Base Random/";
    
    String inputGraphPath = String.format("%s%s.graph", graphDir, inputGraph);
    String inputPtnPath =
      String.format("%s%s.%s.ptn", ptnDir, inputPtn, clusterCount);
    
    NeoFromFile neoGenerator = new NeoFromFile(databaseDir);
    
    try {
      // neoGenerator.generateNeo(inputGraphPath, inputPtnPath);
      neoGenerator.generateNeo(inputGraphPath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    ClusterAlgDiDiC didic = new ClusterAlgDiDiC();
    
    Supervisor didicSupervisor =
      new SupervisorDiDiC(SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);
    
    didic.do_DiDiC(databaseDir, MAX_ITERATIONS, clusterCount,
      ClusterAlgDiDiC.InitType.RANDOM, ClusterAlgDiDiC.AllocType.BASE,
      didicSupervisor);
  }
  
  private static void do_didic_add20_16_base_balanced() {
    int clusterCount = 16;
    
    String inputGraph = "add20";
    String inputPtn = "add20-IN-BAL";
    
    String databaseDir = String.format("var/%s-16-balanced", inputGraph);
    
    String graphDir = "graphs/";
    String ptnDir = "partitionings/";
    String metDir = "metrics/add20 16 Base Balanced/";
    
    String inputGraphPath = String.format("%s%s.graph", graphDir, inputGraph);
    String inputPtnPath =
      String.format("%s%s.%s.ptn", ptnDir, inputPtn, clusterCount);
    
    NeoFromFile neoGenerator = new NeoFromFile(databaseDir);
    
    try {
      // neoGenerator.generateNeo(inputGraphPath, inputPtnPath);
      neoGenerator.generateNeo(inputGraphPath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    ClusterAlgDiDiC didic = new ClusterAlgDiDiC();
    
    Supervisor didicSupervisor =
      new SupervisorDiDiC(SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir, metDir);
    
    didic.do_DiDiC(databaseDir, MAX_ITERATIONS, clusterCount,
      ClusterAlgDiDiC.InitType.BALANCED, ClusterAlgDiDiC.AllocType.BASE,
      didicSupervisor);
  }
  
  private static void do_didic_add20_16_opt_random() {
    int clusterCount = 16;
    
    String inputGraph = "add20";
    String inputPtn = "add20-IN-RAND";
    
    String databaseDir = String.format("var/%s-16-random", inputGraph);
    
    String graphDir = "graphs/";
    String ptnDir = "partitionings/";
    String resultsDir =
      "~/Dropbox/Neo_Thesis_Private/Results/add20 16 Opt Random/";
    
    String inputGraphPath = String.format("%s%s.graph", graphDir, inputGraph);
    String inputPtnPath =
      String.format("%s%s.%s.ptn", ptnDir, inputPtn, clusterCount);
    
    NeoFromFile neoGenerator = new NeoFromFile(databaseDir);
    
    try {
      // neoGenerator.generateNeo(inputGraphPath, inputPtnPath);
      neoGenerator.generateNeo(inputGraphPath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    ClusterAlgDiDiC didic = new ClusterAlgDiDiC();
    
    Supervisor didicSupervisor =
      new SupervisorDiDiC(SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir,
        resultsDir);
    
    didic.do_DiDiC(databaseDir, MAX_ITERATIONS, clusterCount,
      ClusterAlgDiDiC.InitType.DEFAULT, ClusterAlgDiDiC.AllocType.OPT,
      didicSupervisor);
  }
  
  private static void do_didic_add20_16_opt_balanced() {
    int clusterCount = 16;
    
    String inputGraph = "add20";
    String inputPtn = "add20-IN-BAL";
    
    String databaseDir = String.format("var/%s-16-balanced", inputGraph);
    
    String graphDir = "graphs/";
    String ptnDir = "partitionings/";
    String resultsDir =
      "~/Dropbox/Neo_Thesis_Private/Results/add20 16 Opt Balanced/";
    
    String inputGraphPath = String.format("%s%s.graph", graphDir, inputGraph);
    String inputPtnPath =
      String.format("%s%s.%s.ptn", ptnDir, inputPtn, clusterCount);
    
    NeoFromFile neoGenerator = new NeoFromFile(databaseDir);
    
    try {
      // neoGenerator.generateNeo(inputGraphPath, inputPtnPath);
      neoGenerator.generateNeo(inputGraphPath);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
    
    ClusterAlgDiDiC didic = new ClusterAlgDiDiC();
    
    Supervisor didicSupervisor =
      new SupervisorDiDiC(SNAPSHOT_PERIOD, inputGraph, graphDir, ptnDir,
        resultsDir);
    
    didic.do_DiDiC(databaseDir, MAX_ITERATIONS, clusterCount,
      ClusterAlgDiDiC.InitType.DEFAULT, ClusterAlgDiDiC.AllocType.OPT,
      didicSupervisor);
  }
  
}
