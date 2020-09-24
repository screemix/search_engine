package chepuhapp;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

  public static final String query = "query";

  public static class RelevanceMapper extends Mapper<Object, Text, DoubleWritable, Text> {
  @Override
  public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
    StringTokenizer words = new StringTokenizer(text.toString(), "\t");
    
    String dId = words.nextToken().toString();
        
    JSONObject docFreq = new JSONObject(words.nextToken());
    Configuration conf = context.getConfiguration();
    JSONObject queryFreq = new JSONObject(conf.get(query));
    
    double docRelevanceIndex = 0.0;
    Iterator<String> keys = queryFreq.keys();
    while(keys.hasNext()){

      String word = keys.next();
      if (docFreq.has(word)){
        docRelevanceIndex += (Double.parseDouble(queryFreq.get(word).toString()) * Double.parseDouble(docFreq.get(word).toString()));
      }

    }
    context.write(new DoubleWritable(-1 * docRelevanceIndex), new Text(dId));
  }
}

  public static class RelevanceReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
	@Override
    public void reduce(DoubleWritable Key, Iterable<Text> docId, Context context) throws IOException, InterruptedException {
           	for (Text dId: docId) {
                //context.write(dId, new LongWritable(-1 * Key.get()));
           		context.write(dId, new DoubleWritable(-1 * Double.parseDouble(Key.toString())));
           	}
        }

  }
  public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "RelevanceIndexer");
        job.setJarByClass(RelevanceIndexer.class);
        job.setMapperClass(RelevanceMapper.class);
        job.setReducerClass(RelevanceReducer.class);
        
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(getConf());
        Path out = new Path(Paths.OUTPUT_RELEVANCE);

        if ((fs.exists(out) & !fs.delete(out, true)) | (fs.exists(new Path(Paths.OUTPUT_QUERY)) & !fs.delete(new Path(Paths.OUTPUT_QUERY), true))) {
            System.out.println("Output directoty already exists; remove redundant files and restart the job");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_RELEVANCE));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_RELEVANCE));
        String queryRes = QueryVectorizer.queryToVec(args, job.getConfiguration());
        job.getConfiguration().set(query, queryRes);
        int status = job.waitForCompletion(true) ? 0 : 1;
        return status;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RelevanceIndexer(), args);
        ToolRunner.run(new TitleExtractor(), args);
        int status = ToolRunner.run(new SaveFirstN(), args);
        System.exit(status);
        
    }
}
