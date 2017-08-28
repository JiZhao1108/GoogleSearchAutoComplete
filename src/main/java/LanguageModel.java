import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.*;

public class LanguageModel {

    public static class LanguageModelMapper extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;//the phrase will be return if it appearances less than this threshold
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration conf1 = context.getConfiguration();
            threshold = conf1.getInt("threshold", 20);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            //input--> this is cool\t20
            if ((value == null) || (value.toString().trim().length() == 0)) {
                return;
            }

            String line = value.toString().trim();
            String[] words_count = line.split("\t");

            if (words_count.length < 2) {//This is a bad input and should give an exception
                return;
            }

            int count = Integer.parseInt(words_count[words_count.length - 1].trim());
            if (count < threshold) {
                return;
            }

            String[] words = words_count[0].split("\\s+");
            if (words.length < 2) {//has one word only
                return;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                sb.append(words[i] + " ");
            }
            String outputKey = sb.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;
            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static class LanguageModelReducer extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int n;//n is number of the following phrase which will show on the website
        @Override
        public void setup(Context context) throws IOException, InterruptedException {


            Configuration conf2 = context.getConfiguration();
            this.n = conf2.getInt("n", 5);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //use TreeMap to store key and value
            TreeMap<Integer,List<String>> tm = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text value : values) {
                String line = value.toString().trim();
                String[] lastWord_count = line.split("=");
                String lastWord = lastWord_count[0].trim();
                int count = Integer.parseInt(lastWord_count[1].trim());

                if (tm.containsKey(count)) {
                    tm.get(count).add(lastWord);
                }
                else {
                    List<String> list = new ArrayList<String>();
                    list.add(lastWord);
                    tm.put(count, list);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();
            for (int i = 0; i < n && iter.hasNext(); i++) {
                int count = iter.next();
                List<String> lastWords = tm.get(count);
                for (String lastWord : lastWords) {
                    context.write(new DBOutputWritable(key.toString(), lastWord, count), NullWritable.get());

                    if (i >= n) {
                        return;
                    }
                }
            }
        }
    }
}
