import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;


import java.io.IOException;
import java.util.*;

public class Indexer {

    public static class IndexerMap extends Mapper<Object, Text, Text, Text>{
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(text.toString(), "\t");
            
            
            // IDF file
            if (words.countTokens() == 2) {
            	Text word = new Text(words.nextToken());
            	String val = words.nextToken().toString();
            	context.write(word, new Text(val));
            }
            // TF file
            else {
            	Text word = new Text(words.nextToken());
            	String d_id = words.nextToken().toString();
            	String val = words.nextToken().toString();
            	context.write(word, new Text(d_id + "\t" + val));
            	
            }

        }
    }

    public static class IndexerReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        	double Idf = 0.0;
        	
        	for (Text val : values) {
        		StringTokenizer items = new StringTokenizer(val.toString(), "\t");
        		// IDF
        		if (items.countTokens() == 1) {
        			Idf = Double.parseDouble(val.toString());
        			break;
        		}
        	}
        	
        	for (Text val : values) {
        		StringTokenizer items = new StringTokenizer(val.toString(), "\t");
        		// TF
        		if (items.countTokens() == 2) {
        			 Text new_key = new Text(items.nextToken().toString());
        			 Double TfIdf = Double.parseDouble(items.nextToken().toString()) / Idf;
        			 
        			 context.write(new_key, new Text(key.toString() + "\t" + TfIdf.toString()));
        		}
        	}
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "itf");
        job.setJarByClass(Indexer.class);
        job.setMapperClass(IndexerMap.class);
        // job.setCombinerClass(IndexerReducer.class);
        job.setReducerClass(IndexerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}