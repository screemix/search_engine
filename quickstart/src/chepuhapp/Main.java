package chepuhapp;

import org.apache.hadoop.util.ToolRunner;

public class Main {
  public static final String warning = "Wrong input argument's format";
  public static void main(String[] args) throws Exception {

    if (args[0].equals("Index")){
      if (args.length<2){
        System.out.println("Wrong arguments for Index");
        System.exit(-1);

      }
      ToolRunner.run(new TFCounter(), args);
      ToolRunner.run(new IDFCounter(), args);
      ToolRunner.run(new Indexer(), args);
      int status = ToolRunner.run(new DocumentVectorizer(), args);
      System.exit(status);

    }
    else if (args[0].equals("Query")){
      if (args.length < 3){
        System.out.println("Wrong arguments for Query");
        System.exit(-1);
      }
      ToolRunner.run(new RelevanceIndexer(), args);
      ToolRunner.run(new TitleExtractor(), args);
      int status = ToolRunner.run(new SaveFirstN(), args);
      System.exit(status);
    }
    else {
      System.out.println("Input is nor Index nor Query");
      System.exit(-1);
    }

  }
}
