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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Task2Job1 {

    public static class TaxiErrorMapper extends Mapper<Object, Text, Text, Text> {
        private Text taxiID = new Text();
        private Text countPair = new Text();

        // Map function to process each input line
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            try {
                CSVParser parser = CSVParser.parse(line, CSVFormat.DEFAULT);
                CSVRecord record = parser.getRecords().get(0);

                if (record.size() == 17) {
                    String medallion = record.get(0).trim();
                    taxiID.set(medallion);

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
                        countPair.set("1,1");
                    } else {
                        countPair.set("1,0");
                    }

                    context.write(taxiID, countPair);
                }
            } catch (Exception e) {
                // Handle exceptions
            }
        }
    }

    public static class TaxiErrorReducer extends Reducer<Text, Text, Text, FloatWritable> {
        private FloatWritable errorFraction = new FloatWritable();

        // Reduce function to compute the error fraction for each taxi
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalTrips = 0;
            int errorTrips = 0;

            for (Text val : values) {
                String[] counts = val.toString().split(",");
                totalTrips += Integer.parseInt(counts[0]);
                errorTrips += Integer.parseInt(counts[1]);
            }

            if (totalTrips > 0) {
                float fraction = (float) errorTrips / totalTrips;
                errorFraction.set(fraction);
                context.write(key, errorFraction);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task2Job1");
        job.setJarByClass(Task2Job1.class);
        job.setMapperClass(TaxiErrorMapper.class);
        job.setReducerClass(TaxiErrorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.addFileToClassPath(new Path("/home/sharmah100/lib/commons-csv-1.8.jar"));

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}