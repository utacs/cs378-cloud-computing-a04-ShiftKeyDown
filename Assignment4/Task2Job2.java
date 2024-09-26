import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;   
import org.apache.hadoop.mapreduce.Reducer;   
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;   
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

public class Task2Job2 {

    public static class TopTaxiMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private PriorityQueue<TaxiErrorRate> queue;
        private final int TOP_K = 5;

        protected void setup(Context context) throws IOException, InterruptedException {
            queue = new PriorityQueue<>(TOP_K, Comparator.comparingDouble(tr -> tr.errorRate));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2) {
                String taxiID = tokens[0];
                float errorRate = Float.parseFloat(tokens[1]);

                TaxiErrorRate taxiErrorRate = new TaxiErrorRate(taxiID, errorRate);
                queue.add(taxiErrorRate);

                if (queue.size() > TOP_K) {
                    queue.poll();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                TaxiErrorRate tr = queue.poll();
                context.write(new Text(tr.taxiID), new FloatWritable(tr.errorRate));
            }
        }
    }

    public static class TopTaxiReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private PriorityQueue<TaxiErrorRate> queue;
        private final int TOP_K = 5;

        protected void setup(Context context) throws IOException, InterruptedException {
            queue = new PriorityQueue<>(TOP_K, Comparator.comparingDouble(tr -> tr.errorRate));
        }

        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            String taxiID = key.toString();
            float errorRate = 0;
            for (FloatWritable val : values) {
                errorRate = val.get();
                break; // Only one value per key
            }

            TaxiErrorRate taxiErrorRate = new TaxiErrorRate(taxiID, errorRate);
            queue.add(taxiErrorRate);

            if (queue.size() > TOP_K) {
                queue.poll();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                TaxiErrorRate tr = queue.poll();
                context.write(new Text(tr.taxiID), new FloatWritable(tr.errorRate));
            }
        }
    }

    static class TaxiErrorRate {
        String taxiID;
        float errorRate;

        TaxiErrorRate(String taxiID, float errorRate) {
            this.taxiID = taxiID;
            this.errorRate = errorRate;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2Job2");
        job.setJarByClass(Task2Job2.class);
        job.setMapperClass(TopTaxiMapper.class);
        job.setReducerClass(TopTaxiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Output path from Job 1
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Final output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}