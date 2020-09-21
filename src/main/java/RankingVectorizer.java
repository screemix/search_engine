import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class RelevanceIndexer extends Configured implements Tool {

  public static final String query = "query_text";

  public static class RelevanceMapper extends Mapper<Object, Text, DoubleWritable, Text> {

  public void RelevanceMap(Object key, Text text, Context context) throws IOException, InterruptedException {
    StringTokenizer words = new StringTokenizer(text.toString(), "\t");
    Text dId = new Text(words.nextToken());
    JSONObject docFreq = new JSONObject(words.nextToken());
    Configuration conf = context.getConfiguration();
    JSONObject queryFreq = new JSONObject(conf.get("query"));

    double docRelevanceIndex = 0;
    Iterator<String> keys = queryFreq.keys();
    while(keys.hasNext()){

      word = keys.getNext();
      if (docFreq.has(word)){
        docRelevanceIndex += Double.parseDouble(queryFreq.get(word).toString()) * Double.parseDouble(docFreq.get(word).toString());
      }

    }
    context.write(new DoubleWritable(-1 * docRelevanceIndex), new Text(dId));
  }
}

  public static class RelevanceReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    public void reduce(DoubleWritable Rank, Iterable<Text> docId, Context context) throws IOException, InterruptedException {
            Text dId = new Text(docId.iterator().next().toString());
            context.write(dId, new DoubleWritable(-1 * Rank.get()));
        }

  }
  public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "RelevanceIndexer");
        job.setJarByClass(RelevanceIndexer.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setReducerClass(RelevanceReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);


        FileSystem fs = FileSystem.get(getConf());
        Path out = new Path(Paths.RANK_OUT);

        if ((fs.exists(out) & !fs.delete(out, true)) | (fs.exists(new Path(Paths.QUERY_OUT)) & !fs.delete(new Path(Paths.QUERY_OUT), true))) {
            System.out.println("Output directoty already exists; remove redundant files and restart tje job");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(Paths.RANK_IN));
        FileOutputFormat.setOutputPath(job, new Path(Paths.RANK_OUT));
        job.getConfiguration().set(query, QueryVectorizer.queryToVec(args, job.getConfiguration()));
        int status = job.waitForCompletion(true) ? 0 : 1;
        ContentExtractor.run(args, getConf());
        return status;
    }

    public static void main(String[] args) throws Exception {
        int static = ToolRunner.run(new RelevanceAnalyzer(), args);
        System.exit(status);
    }
}
