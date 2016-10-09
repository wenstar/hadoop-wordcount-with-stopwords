/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.fs.FileSystem;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 */
public class wordcount extends Configured implements Tool {

    /**
     * Counts the words in each line.
     * For each line of input, break the line into words and emit them as
     * (<b>word</b>, <b>1</b>).
     */
    public static class MapClass extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        HashSet<String> stopWords = new HashSet<String>();

        public void configure(JobConf job) {
     		/*try{
            	BufferedReader reader = new BufferedReader(new FileReader("stop-words"));
            	String words = reader.readLine();
            	StringTokenizer sitr = new StringTokenizer(words);
            	while (sitr.hasMoreTokens()) {
                stopWords.add(sitr.nextToken());
            	}
        	}catch (IOException e){
        		e.printStackTrace();
        	} 
			System.out.println(stopWords.toString());*/
            String uri = "stop-words";
            try {
                FileSystem fs = FileSystem.get(URI.create(uri), job);
                InputStream in = fs.open(new Path(uri));
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String line = reader.readLine();
                StringTokenizer sitr = new StringTokenizer(line);
                while (sitr.hasMoreTokens()) {
                    stopWords.add(sitr.nextToken());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
			System.out.println(stopWords.toString());
        }

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(!stopWords.contains(word.toString())) {
                    output.collect(word, one);
                }
            }
        }
    }

    /**
     * A reducer class that just emits the sum of the input values.
     */
    public static class Reduce extends MapReduceBase
            implements Reducer<Text, IntWritable, Text, IntWritable> {
        int min = 0;
        public void configure(JobConf job) {
            min = Integer.parseInt(job.get("minsum"));
        }

        public void reduce(Text key, Iterator<IntWritable> values,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            if(min > 0){
                if(sum >= min){
                    output.collect(key, new IntWritable(sum));
                }
            }else{
                output.collect(key, new IntWritable(sum));
            }
        }
    }

    static int printUsage() {
        System.out.println("wordcount [-m <maps>] [-r <reduces>] <min sum> <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    /**
     * The main driver for word count map/reduce program.
     * Invoke this method to submit the map/reduce job.
     * @throws IOException When there is communication problems with the
     *                     job tracker.
     */
    public int run(String[] args) throws Exception {
        HashSet<String> stopWords = new HashSet<String>();

        JobConf conf = new JobConf(getConf(), wordcount.class);
        conf.setJobName("wordcount");
        // the keys are words (strings)
        conf.setOutputKeyClass(Text.class);
        // the values are counts (ints)
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(MapClass.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-m".equals(args[i])) {
                    conf.setNumMapTasks(Integer.parseInt(args[++i]));
                } else if ("-r".equals(args[i])) {
                    conf.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else if ("-min".equals(args[i])) {
                    conf.set("minsum" , args[++i]);
                }else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(conf, other_args.get(0));
        FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

        JobClient.runJob(conf);
        return 0;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new wordcount(), args);
        System.exit(res);
    }

}

