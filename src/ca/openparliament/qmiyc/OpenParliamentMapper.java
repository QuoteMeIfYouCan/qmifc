package ca.openparliament.qmiyc;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import ca.openparliament.qmiyc.models.OpenParliamentRecord;

public abstract class OpenParliamentMapper<K extends WritableComparable<?>, V extends Writable>
extends ModelMapper<OpenParliamentRecord, LongWritable, Text, K, V> {

	/**
	 * Configures the MapReduce job to read data from the NASDAQ dump from Infochimps.
	 *
	 * @param job
	 */
	public static void configureJob(Job job) {
		job.setInputFormatClass(TextInputFormat.class);
	}

	@Override
	protected OpenParliamentRecord instantiateModel(LongWritable key, Text value) {
		return new OpenParliamentRecord(value);
	}

}