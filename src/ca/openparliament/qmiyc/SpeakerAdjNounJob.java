package ca.openparliament.qmiyc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import opennlp.maxent.Counter;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.InvalidFormatException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ca.openparliament.qmiyc.models.OpenParliamentRecord;

/**
 * This MapReduce job will read the NASDAQ or NYSE daily prices dataset and
 * output the highest market caps obtained by each Stock symbol.
 * 
 */
public class SpeakerAdjNounJob extends Configured implements Tool {

	public static class SpeakerNounWritable implements
			WritableComparable<SpeakerNounWritable> {

		private String speaker;
		private String nouns;

		public SpeakerNounWritable() {
			this.speaker = "UNSET";
			this.nouns = "UNSET";
		}

		public SpeakerNounWritable(String speaker, String nouns) {
			this.speaker = speaker;
			this.nouns = nouns;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.speaker = in.readUTF();
			this.nouns = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(speaker);
			out.writeUTF(nouns);
		}

		public String getSpeaker() {
			return speaker;
		}

		public String getNouns() {
			return nouns;
		}

		@Override
		public int compareTo(SpeakerNounWritable other) {
			int s = this.speaker.compareTo(other.speaker);
			if (s == 0)
				return this.nouns.compareTo(other.nouns);
			return s;
		}

	}

	public static class AdjCountWritable implements
			WritableComparable<AdjCountWritable> {

		private String adj;
		private int count;

		public AdjCountWritable() {
			this.adj = "UNSET";
			this.count = 0;
		}

		public AdjCountWritable(String adj, int count) {
			this.adj = adj;
			this.count = count;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.adj = in.readUTF();
			this.count = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(adj);
			out.writeInt(count);
		}

		public String getAdj() {
			return adj;
		}

		public int getCount() {
			return count;
		}

		@Override
		public int compareTo(AdjCountWritable other) {
			int s = this.adj.compareTo(other.adj);
			if (s == 0)
				return Integer.valueOf(this.count).compareTo(other.count);
			return s;
		}

	}

	public static class SpeakerAdjNounMapper extends
			OpenParliamentMapper<SpeakerNounWritable, AdjCountWritable> {

		protected POSTaggerME tagger;

		public SpeakerAdjNounMapper() {
			POSModel model;
			try {
				model = new POSModel(
						ClassLoader
								.getSystemResourceAsStream("en-pos-maxent.bin"));
				// new POSModelLoader().load(new File(args[0]));
			} catch (InvalidFormatException e) {
				e.printStackTrace();
				return;
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			tagger = new POSTaggerME(model);
		}

		@Override
		protected void map(OpenParliamentRecord record, Context context)
				throws IOException, InterruptedException {

			String whitespaceTokenizerLine[] = WhitespaceTokenizer.INSTANCE
					.tokenize(record.getContentEn());
			String[] tags = tagger.tag(whitespaceTokenizerLine);

			// get noun islands
			List<int[]> nouns = new ArrayList<int[]>();
			int nounStart = 0;
			boolean inNoun = false;
			for (int i = 0; i < tags.length; i++) {
				boolean isNoun = tags[i].startsWith("NN");
				if (isNoun && !inNoun) {
					nounStart = i;
					inNoun = true;
				} else if (!isNoun && inNoun) {
					nouns.add(new int[] { nounStart, i - nounStart });
					inNoun = false;
				}
			}
			if (inNoun) {
				nouns.add(new int[] { nounStart, tags.length - nounStart });
			}

			// find adjectives near-by
			int last = 0;
			for (int[] noun : nouns) {
				int prev = noun[0];
				for (; prev - 1 > last && tags[prev - 1].startsWith("JJ"); prev--)
					;
				if (prev != noun[0]) {
					StringBuilder sb = new StringBuilder();
					for (int i = 0; i < noun[1]; i++)
						sb.append(i == 0 ? "" : " ").append(
								whitespaceTokenizerLine[i]);
					String nounsStr = sb.toString();
					sb.setLength(0);
					for (int i = prev; i < noun[0]; i++)
						sb.append(i == 0 ? "" : " ").append(
								whitespaceTokenizerLine[i]);
					String adjsStr = sb.toString();

					context.write(new SpeakerNounWritable(record.getWho(),
							nounsStr), new AdjCountWritable(adjsStr, 1));
				}
			}
		}
	}

	public static class SpeakerAdjNounReducer extends
			Reducer<SpeakerNounWritable, AdjCountWritable, Text, Text> {

		@Override
		protected void reduce(SpeakerNounWritable key,
				Iterable<AdjCountWritable> values, Context context)
				throws IOException, InterruptedException {
			// adj to counts
			Map<String, Counter> counters = new HashMap<String, Counter>();
			for (AdjCountWritable adjC : values) {
				String adj = adjC.getAdj();
				if (!counters.containsKey(adj))
					counters.put(adj, new Counter());
				counters.get(adj).increment();
			}
			List<String> adjs = new ArrayList<String>(counters.keySet());
			Collections.sort(adjs);
			for (String adj : adjs) {
				context.write(
						new Text(key.getSpeaker() + "\t" + key.getNouns()),
						new Text(adj + "\t" + counters.get(adj).intValue()));
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		if (args.length != 2) {
			System.err.println("Usage: " + getClass().getName()
					+ " <input> <output>");
			System.exit(2);
		}

		// Creating the MapReduce job (configuration) object
		Job job = new Job(conf);
		job.setJarByClass(getClass());
		job.setJobName(getClass().getName());

		// Tell the job which Mapper and Reducer to use (classes defined above)
		job.setMapperClass(SpeakerAdjNounMapper.class);
		job.setReducerClass(SpeakerAdjNounReducer.class);

		SpeakerAdjNounMapper.configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(SpeakerNounWritable.class);
		job.setMapOutputValueClass(AdjCountWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Setting the input folder of the job
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
		Path output = new Path(args[1]);
		FileSystem.get(conf).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(),
				new SpeakerAdjNounJob(), args);
		System.exit(result);
	}

}
