package chepuhapp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;  

public class TFCounter extends Configured implements Tool {

    public static class TFCounterMap extends Mapper<Object, Text, Text, IntWritable>{
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            JSONObject json = null;
            try {
                json = new JSONObject(text.toString());
                Text content = new Text(json.get("text").toString());
                // String title = json.get("title") .toString().replaceAll(","," ");
                String d_id = json.get("id").toString();
                // StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
                StringTokenizer words = new StringTokenizer(content.toString().toLowerCase().replaceAll("[^A-Za-z- ]", ""));

                while (words.hasMoreTokens()) {
                    String word = words.nextToken().toLowerCase();
                    if (!word.trim().equals("") && !(word.charAt(0) == '-')) {
                        context.write(new Text(word + "\t" + d_id), one);
                    }

                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }
    }

    public static class TFCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int term_frequency = 0;
            for (IntWritable val : values) {
                term_frequency+=val.get();
            }
            result.set(term_frequency);
            context.write(key, result);
        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "term frequency");
        job.setJarByClass(TFCounter.class);
        job.setMapperClass(TFCounterMap.class);
        job.setCombinerClass(TFCounterReducer.class);
        job.setReducerClass(TFCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_TF));
		return job.waitForCompletion(true) ? 0 : 1;
    }
}