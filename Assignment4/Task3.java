import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;   
import org.apache.hadoop.mapreduce.Reducer;   
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;   
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Task3 {

    public static class DriverEarningsMapper extends Mapper<Object, Text, Text, Text> {
        private Text driverID = new Text();
        private Text earningsAndTime = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            try {
                CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT);
                CSVRecord record = parser.getRecords().get(0);

                if (record.size() == 17) {
                    String driver = record.get(1).trim();
                    driverID.set(driver);

                    // Extract trip time in seconds and total amount
                    double tripTimeSecs = Double.parseDouble(record.get(4).trim());
                    double totalAmount = Double.parseDouble(record.get(16).trim());

                    // Convert trip time to minutes
                    double tripTimeMins = tripTimeSecs / 60.0;

                    // Check for valid trip time
                    if (tripTimeMins > 0) {
                        // Emit (driverID, "totalAmount,tripTimeMins")
                        earningsAndTime.set(totalAmount + "," + tripTimeMins);
                        context.write(driverID, earningsAndTime);
                    }
                }
            } catch (Exception e) {
                // Handle parsing exceptions
            }
        }
    }

    public static class DriverEarningsReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private PriorityQueue<DriverEarnings> queue;
        private final int TOP_K = 10;

        protected void setup(Context context) throws IOException, InterruptedException {
            queue = new PriorityQueue<>(TOP_K, Comparator.comparingDouble(de -> de.earningsPerMinute));
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double totalEarnings = 0.0;
            double totalTime = 0.0;

            for (Text val : values) {
                String[] earningsAndTime = val.toString().split(",");
                totalEarnings += Double.parseDouble(earningsAndTime[0]);
                totalTime += Double.parseDouble(earningsAndTime[1]);
            }

            if (totalTime > 0) {
                double earningsPerMinute = totalEarnings / totalTime;
                DriverEarnings de = new DriverEarnings(key.toString(), earningsPerMinute);
                queue.add(de);

                if (queue.size() > TOP_K) {
                    queue.poll();
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!queue.isEmpty()) {
                DriverEarnings de = queue.poll();
                context.write(new Text(de.driverID), new DoubleWritable(de.earningsPerMinute));
            }
        }
    }

    static class DriverEarnings {
        String driverID;
        double earningsPerMinute;

        DriverEarnings(String driverID, double earningsPerMinute) {
            this.driverID = driverID;
            this.earningsPerMinute = earningsPerMinute;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task3");
        job.setJarByClass(Task3.class);
        job.setMapperClass(DriverEarningsMapper.class);
        job.setReducerClass(DriverEarningsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Add external library to classpath
        job.addFileToClassPath(new Path("/home/your_username/lib/commons-csv-1.8.jar"));

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}