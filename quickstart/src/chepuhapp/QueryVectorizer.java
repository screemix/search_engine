package chepuhapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.json.JSONObject;

public class QueryVectorizer {

  public static String queryToVec(String[] args, Configuration configuration) throws Exception {

    Map<String, Double> querySet = new HashMap<String, Double>();
    String query = args[args.length - 1].toLowerCase();
    StringTokenizer queryItems = new StringTokenizer(query, " \'\n.,!?:()[]{};\\/\"*");

    //vectorizing query  to form {word: num_of_occurrences_in_query}
    while (queryItems.hasMoreTokens()) {
      String word = queryItems.nextToken().toString();
      if (querySet.containsKey(word)){
        querySet.put(word, querySet.get(word)+1.0);
      }
      else{
        querySet.put(word, 1.0);
      }
    }

    //Loading IDF values from vocabulary
    FileSystem fs = FileSystem.get(configuration);
    FSDataInputStream IDFfile = fs.open(new Path(Paths.INPUT_INDEXER_IDF));
    BufferedReader BufRead = new BufferedReader(new InputStreamReader(IDFfile));

    String line = BufRead.readLine();
    while (line != null){
      StringTokenizer IdfWords = new StringTokenizer(line, "\t");
      String word = IdfWords.nextToken().toString();
      double idf = Double.parseDouble(IdfWords.nextToken().toString());

      if (querySet.containsKey(word)){
        querySet.put(word, querySet.get(word)/idf);
      }
      line = BufRead.readLine();
    }

    JSONObject res = new JSONObject();
    for (String key : querySet.keySet()) {
        String value = querySet.get(key).toString();
        res.put(key, value);
    }
    String result = res.toString();
   

    return (result);

  }

}
