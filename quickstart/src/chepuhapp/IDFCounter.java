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
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class IDFCounter extends Configured implements Tool {

    public static class IDFCounterMap extends Mapper<Object, Text, Text, IntWritable>{
    	
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            JSONObject json = null;
            try {
                json = new JSONObject(text.toString());
                Text content = new Text(json.get("text").toString());
                String d_id = json.get("id").toString();
                // StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
                StringTokenizer words = new StringTokenizer(content.toString().toLowerCase().replaceAll("[^A-Za-z- ]", ""));
                Set<String> vocab = new HashSet<String>();

                while (words.hasMoreTokens()) {
                    String word = words.nextToken().toLowerCase();
                    String tmp = word + "_" + d_id;
                    if (!vocab.contains(tmp) && !word.equals("") && !(word.charAt(0) == '-')) {
                        vocab.add(tmp);
                        context.write(new Text(word), one);
                    }

                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }
    }

    public static class IDFCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int DocCount = 0;
            for (IntWritable val : values) {
                DocCount += val.get();
;            }
            result.set(DocCount);
            context.write(key, result);

        }

    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "idf counter");
        job.setJarByClass(IDFCounter.class);
        job.setMapperClass(IDFCounterMap.class);
        job.setCombinerClass(IDFCounterReducer.class);
        job.setReducerClass(IDFCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(Paths.OUTPUT_IDF));
		return job.waitForCompletion(true) ? 0 : 1;
    }
}