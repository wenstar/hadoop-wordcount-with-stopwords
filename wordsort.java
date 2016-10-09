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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i>
 */
public class wordsort extends Configured implements Tool {


	private static class Comparator extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), wordsort.class);
        conf.setJobName("wordsort");
        // the keys are words (strings)
        conf.setOutputKeyClass(IntWritable.class);
        // the values are counts (ints)
        conf.setOutputValueClass(Text.class);
        conf.setNumReduceTasks(1);

        conf.setMapperClass(InverseMapper.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputKeyComparatorClass(Comparator.class);

        FileInputFormat.setInputPaths(conf, "temp/part-00000");
        FileOutputFormat.setOutputPath(conf,  new Path("output"));

        JobClient.runJob(conf);

        return 0;
    }

	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new wordsort(), args);
        System.exit(res);
    }

}

