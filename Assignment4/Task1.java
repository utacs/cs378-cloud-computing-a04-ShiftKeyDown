import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;   
import org.apache.hadoop.mapreduce.Reducer;   
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;   
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Task1 {

    public static class Task1Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable hourKey = new IntWritable();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Map function to process each input line
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            try {
                // Parse the CSV line
                CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT);
                CSVRecord record = parser.getRecords().get(0);

                if (record.size() == 17) {
                    // Extract pickup datetime
                    String pickupDateStr = record.get(2).trim();
                    String pickupTimeStr = record.get(3).trim();
                    String pickupDateTimeStr = pickupDateStr + " " + pickupTimeStr;
                    Date pickupDateTime = dateFormat.parse(pickupDateTimeStr);
                    int hour = pickupDateTime.getHours() + 1;
                    hourKey.set(hour);

                    // Check GPS fields
                    boolean gpsError = false;
                    for (int i = 6; i <= 9; i++) {
                        String gpsValue = record.get(i).trim();
                        if (gpsValue.isEmpty() || gpsValue.equals("0") || gpsValue.equals("0.0")) {
                            gpsError = true;
                            break;
                        }
                    }

                    if (gpsError) {
                        context.write(hourKey, one);
                    }
                }
            } catch (ParseException e) {
                // Skip invalid date formats
            } catch (Exception e) {
                // Handle parsing exceptions
            }
        }
    }

    public static class Task1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        // Reduce function to sum values for each hour key
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(Task1Mapper.class);
        job.setCombinerClass(Task1Reducer.class);
        job.setReducerClass(Task1Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // Add external library to classpath
        job.addFileToClassPath(new Path("/home/sharmah100/lib/commons-csv-1.8.jar"));

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}