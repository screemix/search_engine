import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TFCounter {

    public static class TFCounterMap extends Mapper<Object, Text, Text, IntWritable>{
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            JSONObject json = null;
            try {
                json = new JSONObject(text.toString());
                Text content = new Text(json.get("text").toString());
                String title = json.get("title") .toString().replaceAll(","," ");
                String d_id = json.get("id").toString() + " " + title;
                StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");

                while (words.hasMoreTokens()) {
                    String word = words.nextToken().toLowerCase();
                    if (!word.equals("")) {
                        context.write(new Text(word + "_" + d_id), one);
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "itf");
        job.setJarByClass(TFCounter.class);
        job.setMapperClass(TFCounterMap.class);
        job.setCombinerClass(TFCounterReducer.class);
        job.setReducerClass(TFCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}