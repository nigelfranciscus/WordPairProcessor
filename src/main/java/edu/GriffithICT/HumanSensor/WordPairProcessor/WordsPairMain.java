package edu.GriffithICT.HumanSensor.WordPairProcessor;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.io.MongoUpdateWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

import redis.clients.jedis.Jedis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import java.net.UnknownHostException;

public class WordsPairMain extends MongoTool {

	public static Jedis jedis;
	public static String dbName, inputColName, outputColname;

	public WordsPairMain(String[] pArgs) throws UnknownHostException {


		// set up redis
		jedis = new Jedis("localhost");
		dbName = pArgs[0];
		inputColName = pArgs[1];
		outputColname = pArgs[2];


		// handle mapreduce task from mongo
		Configuration conf = new Configuration();
		setConf(conf);
		boolean mrv1Job;
		try {
			FileSystem.class.getDeclaredField("DEFAULT_FS");
			mrv1Job = false;
		} catch (NoSuchFieldException e) {
			mrv1Job = true;
		}
		if (mrv1Job) {
			MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
			MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
		} else {
			MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
			MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
		}

        MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/" + dbName + "." + inputColName);
        MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/" + dbName + "." + outputColname);

		MongoConfigUtil.setMapper(getConf(), WordsPairMapper.class);
		MongoConfigUtil.setReducer(getConf(), WordsPairReducer.class);

		MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
		MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);

		/*
		 * Format for write mongo object
		 */
		MongoConfigUtil.setOutputKey(getConf(), NullWritable.class);
		 
		Class outputValueClass = MongoUpdateWritable.class;
		MongoConfigUtil.setOutputValue(getConf(), outputValueClass);
	}

	public static void main(String[] pArgs) throws Exception {

		if (pArgs.length != 5) {
			System.out.println(
					"Usage: hadoop jar /path/.jar <db> <inputCollection> <outputCollection> <dictionaryFile> <ignoredWordsFile>");
			return;
		}

		WordsPairMapper.dictionaryFile = pArgs[3];
		WordsPairMapper.ignoredWordsFile = pArgs[4];

		System.exit(ToolRunner.run(new WordsPairMain(pArgs), pArgs));
	}
}