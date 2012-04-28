package ca.openparliament.qmiyc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

/**
 * Count the number of time each context is appearing with a named entity.
 *
 * The input must is a tab separated list of: speaker, named entity, context, frequency.
 *
 * The output is a tab separated list of: BACKGROUND, named entity, context, frequency.
 */
public class ContextCounterJob extends Configured implements Tool {
    public static final String BACKGROUND_SPEAKER = "BACKGROUND";

    static class ContextCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            for (LongWritable value : values) {
                sum += value.get();
            }

            context.write(key, new LongWritable(sum));
        }
    }

    static class ContextCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] attributes = value.toString().split("\t", 4);

            // attribute 0 is speaker, we do not use it
            final String ne = attributes[1];
            final String ctx = attributes[2];
            final long count = Long.parseLong(attributes[3]);

            context.write(new Text(String.format("%s\t%s\t%s", BACKGROUND_SPEAKER, ne, ctx)), new LongWritable(count));
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
        job.setMapperClass(ContextCounterMapper.class);
        job.setReducerClass(ContextCounterReducer.class);

        // This is what the Mapper will be outputting to the Reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // This is what the Reducer will be outputting
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Setting the input folder of the job
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new ContextCounterJob(), args);
        System.exit(result);
    }
}
