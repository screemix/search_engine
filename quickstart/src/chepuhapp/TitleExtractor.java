package chepuhapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;

public class TitleExtractor extends Configured implements Tool {

    public static class TitleMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(text.toString(), "\t");
            
            // Query file
            if (words.countTokens() == 2) {
            	Text doc_id = new Text(words.nextToken());
            	String freq = words.nextToken().toString();
            	context.write(doc_id, new Text(freq));
            	//context.write(new Text("FILE"), new Text("kek"));
            }
            // JSON file
            else {
            	JSONObject json = null;
            	json = new JSONObject(text.toString());
            	String d_id = json.get("id").toString();
            	String title = json.get("title").toString();
            	context.write(new Text(d_id), new Text("!" + title));
            	//context.write(new Text("JSON"), new Text("lmao"));
            }

        }
    }

    public static class TitleReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	char c = '!';
    		String title = "";
    		String score = "";
        	
        	
        	for (Text val : values) {
        		if (val.toString().charAt(0) == c) {
        			title = val.toString().substring(1);
        		}
        		else {
        			score = val.toString();
        		}
        	}
        	
        	if (!score.isEmpty()) {
        		context.write(new Text(score), new Text(key.toString() + "\t" + title));
        	}
        		
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "title extractor");
        job.setJarByClass(TitleExtractor.class);
        job.setMapperClass(TitleMapper.class);
        // job.setCombinerClass(IndexerReducer.class);
        job.setReducerClass(TitleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_EXTRACTOR));
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_EXTRACTOR));
        int status = job.waitForCompletion(true) ? 0 : 1;
        
        return status;
    }
}