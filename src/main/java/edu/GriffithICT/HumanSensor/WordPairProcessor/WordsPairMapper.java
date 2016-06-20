package edu.GriffithICT.HumanSensor.WordPairProcessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

public class WordsPairMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

	private static final Log LOG = LogFactory.getLog(WordsPairMapper.class);

	public static String dictionaryFile;
	public static String ignoredWordsFile;

	private final Text keyText;
	private final IntWritable valueOne;

	private Set<String> dictionary;
	private Set<String> ignoredWords;

	public WordsPairMapper() {
		super();

		keyText = new Text();
		valueOne = new IntWritable(1);
		dictionary = new HashSet<String>();
		ignoredWords = new HashSet<String>();

		buidDictionary();
		buildIgnoredSet();
	}

	@Override
	public void map(final Object key, final BSONObject val, final Context context)
			throws IOException, InterruptedException {
		// 20151204060000, 0 to 4 means we want the highest temperature for each
		// year

		try {
			String text = (String) val.get("text");
			if (text != null) {

				StringTokenizer itr = new StringTokenizer(text);
				List<String> words = new ArrayList<String>();

				// get all the words from this twit line
				while (itr.hasMoreTokens()) {
					String word = itr.nextToken().toLowerCase();
					if (!ignoredWords.contains(word) && dictionary.contains(word)) {
						words.add(word);
					}
				}
				Collections.sort(words);
				// for each pair of word
				for (int i = 0; i < words.size(); i++) {
					for (int j = i + 1; j < words.size(); j++) {
						if(!words.get(i).equals(words.get(j))){
							String wordPairKey = words.get(i) + "_" + words.get(j);
							keyText.set(wordPairKey);
							context.write(keyText, valueOne);	
						}
					}
				}
			}

		} catch (NumberFormatException e) {
			LOG.info("Text Object null");
		}

	}

	private void buidDictionary() {
		File file = new File(dictionaryFile);

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (line.trim().length() > 0) {
					dictionary.add(line.trim());
				}
			}
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void buildIgnoredSet() {
		File file = new File(ignoredWordsFile);

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (line.trim().length() > 0) {
					ignoredWords.add(line.trim());
				}
			}
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
