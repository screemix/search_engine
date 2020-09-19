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

public class ITFCounter {

    public static class ITFCounterMap extends Mapper<Object, Text, Text, IntWritable>{
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text text, Context context) throws IOException, InterruptedException {
            JSONObject json = null;
            try {
                json = new JSONObject(text.toString());
                Text content = new Text(json.get("text").toString());

                StringTokenizer words = new StringTokenizer(content.toString(), " \'\n.,!?:()[]{};\\/\"*");
                Set<String> vocab = new HashSet<String>();

                while (words.hasMoreTokens()) {
                    String word = words.nextToken().toLowerCase();
                    if (!vocab.contains(word)) {
                        vocab.add(word);
                        context.write(new Text(word), one);
                    }

                }
            } catch (JSONException e) {
                e.printStackTrace();
            }

        }
    }

    public static class ITFCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int DocCount = 0;
            for (IntWritable val : values) {
                DocCount++;
            }
            result.set(DocCount);
            context.write(key, result);

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "itf");
        job.setJarByClass(ITFCounter.class);
        job.setMapperClass(ITFCounterMap.class);
        job.setCombinerClass(ITFCounterReducer.class);
        job.setReducerClass(ITFCounterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}