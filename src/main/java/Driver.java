import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

        String inputDir = args[0];
        String outputDir = args[1];
        String NGramNum = args[2];
        String threshold = args[3];
        String NumOfFollowingPhrase = args[4];

        Configuration conf1 = new Configuration();
        conf1.set("textinputformat.record.delimiter",".");
        conf1.set("NGramNum", NGramNum);

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGramLibrary");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibrary.NGramLibMapper.class);
        job1.setReducerClass(NGramLibrary.NGramLibReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(outputDir));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("threshold", threshold);
        conf2.set("NumOfFollowingPhrase", NumOfFollowingPhrase);

        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://10.200.143.140:8889/test5",//your ip address
                "root",
                "root");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("LanguageModel");
        job2.setJarByClass(Driver.class);

        //shortage
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));
        job2.setMapperClass(LanguageModel.LanguageModelMapper.class);
        job2.setReducerClass(LanguageModel.LanguageModelReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        TextInputFormat.setInputPaths(job2, new Path(outputDir));
        DBOutputFormat.setOutput(job2, "output",
                new String[]{"starting_phrase", "following_word", "count"});
        job2.waitForCompletion(true);
    }
}
