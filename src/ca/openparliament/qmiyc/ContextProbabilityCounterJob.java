package ca.openparliament.qmiyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Compute the probability of a context word given a speaker and a named entity.
 */
public class ContextProbabilityCounterJob extends Configured implements Tool {
    static class ContextProbabilityCounterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] attributes = value.toString().split("\t");
            final String speaker = attributes[0];
            final String ne = attributes[1];
            final String ctx = attributes[2];
            final String freq = attributes[3];

            context.write(new Text(String.format("%s\t%s", speaker, ne)),
                    new Text(String.format("%s\t%s", ctx, freq)));
        }
    }

    static class ContextProbabilityCounterReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            Map<String, Long> counts = new HashMap<String, Long>();

            for (Text value : values) {
                final String[] attributes = value.toString().split("\t");
                final String ctx = attributes[0];
                final long freq = Long.parseLong(attributes[1]);
                counts.put(ctx, freq);
                sum += freq;
            }

            for (Map.Entry<String, Long> count : counts.entrySet()) {
                final double prob = (double) (count.getValue()) / sum;
                final String ctx = count.getKey();

                context.write(new Text(String.format("%s\t%s", key, ctx)),
                        new DoubleWritable(Math.log(prob)));
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length != 2) {
            System.err.println("Usage: " + getClass().getName() + " <input> <output>");
            System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        job.setInputFormatClass(TextInputFormat.class);

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(ContextProbabilityCounterMapper.class);
        job.setReducerClass(ContextProbabilityCounterReducer.class);

        // This is what the Mapper will be outputting to the Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // This is what the Reducer will be outputting
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Setting the input folder of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ContextProbabilityCounterJob(), args);
        System.exit(result);
    }

}
