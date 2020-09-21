import org.apache.hadoop.util.ToolRunner;
import utils.InversedDocumentFrequency;
import utils.RelevanceAnalyzer;
import utils.TermFrequency;

public class Main {
  public public static final String warning = "Wrong input argument's format";
  public static void main(String[] args) throws Exception {

    if (args[0].equal("Query")){
      if (args.length<3){
        System.out.println(warning);
        System.exit(-1);

      }
      ToolRunner.run(new TDCounter(), args);
      ToolRunner.run(new ITFCounter(), args);
      ToolRunner.run(new Indexer(), args);
      int status = ToolRunner.run(new DocumentVectorizer(), args);
      System.exit(status);

    }
    else if (args[0].equal("Indexer")){
      if (args.length<2){
        System.out.println(warning);
        System.exit(-1);
      }
      int status = ToolRunner.run(new RankingVectorizer(), args);
      System.exit(resultOfJob);
    }
    else {
      System.out.println(warning);
      System.exit(-1);
    }

  }
}
