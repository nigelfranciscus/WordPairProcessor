package edu.GriffithICT.HumanSensor.WordPairProcessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.io.BSONWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordsPairReducer extends Reducer<Text, IntWritable, NullWritable, BSONWritable> {

	private static final Log LOG = LogFactory.getLog(WordsPairReducer.class);

	@Override
	public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		for (final IntWritable val : values) {
			sum += val.get();
		}

		// set the threshold, frequencel less then this number will be filtered
		if (sum > 1) {
			// write to mongo as the reducer
			String[] wordPair = key.toString().split("_");
			DBObject builder = new BasicDBObjectBuilder().start().add("word1", wordPair[0]).add("word2", wordPair[1])
					.add("frequency", sum).get();
			BSONWritable doc = new BSONWritable(builder);
			context.write(NullWritable.get(), doc);
		}
	}

}
