package chepuhapp;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class SaveFirstN extends Configured implements Tool {
	public static String getN = "N";
	public static int N = 0;
	public static String result = "RANK\tID\tTITLE\n";
	
    public static class SaveFirstNMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
        	
        	StringTokenizer words = new StringTokenizer(text.toString(), "\t");
        	Double rank = Double.parseDouble(words.nextToken());
        	String id = words.nextToken().toString();
        	String title = words.nextToken().toString();
    		
    		context.write(new DoubleWritable(-1 * rank), new Text(id + "\t" + title));

        }
    }
    
    public static class SaveFirstNReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
        	if (N > 0) {
	        	
	        	for (Text val : values) {
	        		double true_key = -1 * Double.parseDouble(key.toString());
	        		
	        		context.write(new DoubleWritable(true_key), val);
	        		result += Double.toString(true_key)+ "\t" + val.toString() + "\n";
	        	}
	        	
	        	N--;
        	}
        	
        }

    }
	
	public int run(java.lang.String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "get topn");
        job.setJarByClass(SaveFirstN.class);
        job.setMapperClass(SaveFirstNMapper.class);
        job.setReducerClass(SaveFirstNReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(Paths.INPUT_TOPN));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_TOPN));
		N = Integer.parseInt(args[1]);
		int res = job.waitForCompletion(true) ? 0 : 1;
		System.out.println(result);
		return res;
	}
	
	
	
	
	
	
	
	
	
}