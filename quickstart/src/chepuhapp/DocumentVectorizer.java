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
import java.util.StringTokenizer;  

public class DocumentVectorizer extends Configured implements Tool {

    public static class DocumentVectorizerMap extends Mapper<Object, Text, Text, Text>{
    	
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
        	
        	String line = text.toString();
            StringTokenizer words = new StringTokenizer(line, "\t");
            Text doc_id = new Text(words.nextToken().toString());
            Text content = new Text(words.nextToken().toString()+"\t"+words.nextToken().toString());           
            context.write(doc_id, content);
        }
    }

    public static class DocumentVectorizerReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	JSONObject result = new JSONObject();
        	//String result = "";
        	
        	for (Text val : values) {
        		//StringTokenizer items = new StringTokenizer(val.toString(), "\t");
        		String[] items = val.toString().split("\t");
        		result.put(items[0], items[1]);
        		//result += items[0] + ":" + items[1] + " ";
        	}
    
        	//context.write(key, new Text(result));
        	context.write(key, new Text(result.toString()));
        	
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "document vectorizer");
        job.setJarByClass(DocumentVectorizer.class);
        job.setMapperClass(DocumentVectorizerMap.class);
        //job.setCombinerClass(DocumentVectorizerReducer.class);
        job.setReducerClass(DocumentVectorizerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_DOC_VECTORIZER));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_DOC_VECTORIZER));
        return job.waitForCompletion(true) ? 0 : 1;
    }
}