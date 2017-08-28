import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibrary {

    public static class NGramLibMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int NGramNum;//give NGram a default value
        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            this.NGramNum = conf.getInt("NGramNum", 10);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]", " ");
            String[] words = line.split("\\s+");

            if (words.length < 2) {
                return;
            }
            StringBuilder sb;
            for (int i = 0; i < words.length - 1; i++) {
                sb = new StringBuilder();
                sb.append(words[i]);
                for (int j = 1; j <= this.NGramNum && i + j < words.length; j++) {
                    sb.append(" ");
                    sb.append(words[i + j]);
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramLibReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();//calculate total count of the key
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
