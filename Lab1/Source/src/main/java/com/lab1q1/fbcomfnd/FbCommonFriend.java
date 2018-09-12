package com.lab1q1.fbcomfnd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FbCommonFriend extends Configured implements Tool {

	// Mapper Class which takes each line from input file and generates key and
	// values as (user1,user2) (list of friends)
	public static class FCFMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text();
		private Text fnds = new Text();
		public static int count = 0;

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();

			// separating user and friends by using split function
			String[] lineFromInput = line.split("->");
			String user = lineFromInput[0].trim();
			fnds.set(lineFromInput[1]);
			StringTokenizer itr = new StringTokenizer(lineFromInput[1]);
			while (itr.hasMoreTokens()) {
				String user2 = itr.nextToken().trim();
				// To sort users in alphabetical order
				if (user.compareTo(user2) < 0)
					word.set(user + "," + user2);
				else
					word.set(user2 + "," + user);
				output.collect(word, fnds);

			}
		}
	}

	// Reducer Class to find common friends between the two users using if
	// condition
	public static class FCFReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public static int count = 0;

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			ArrayList<String[]> arr = new ArrayList<String[]>();

			while (values.hasNext()) {
				String temp = values.next().toString();
				String[] listFriend = temp.split(" ");
				arr.add(listFriend);
			}
			String commonFriends = " -> ";
			// find common friend
			HashSet<String> commonElements = new HashSet<String>();
			for (String[] friends : arr) {
				Arrays.sort(friends);
				for (String friend : friends) {

					if (commonElements.contains(friend)) {

						commonFriends += friend + " ";
					} else
						commonElements.add(friend);
				}
			}

			Text commonFriendText = new Text();

			commonFriendText.set(commonFriends);
			output.collect(key, commonFriendText);
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), FbCommonFriend.class);
		conf.setJobName("FB CommonFriend");

		conf.setOutputKeyClass(Text.class);

		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FCFMapper.class);

		conf.setReducerClass(FCFReducer.class);

		FileInputFormat.setInputPaths(conf, args[0]);
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new FbCommonFriend(),
				args);

		System.exit(res);
	}

}