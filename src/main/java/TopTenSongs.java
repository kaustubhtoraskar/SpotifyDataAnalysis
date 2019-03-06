import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;

public class TopTenSongs extends Configured implements Tool {

    private static int N = 10 ;

    public static class MapClass
            extends Mapper<LongWritable, Text, Text, Text> {

        private TreeMap<Double, Text> TopKMap = new TreeMap<Double, Text>() ;

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            try {

                String[] line = value.toString().split(",");
                Double e = Double.parseDouble(line[4]);

                TopKMap.put(new Double(e), new Text(line[1]));
                //Line[1] is the name of the song

                if (TopKMap.size() > N) {
                    TopKMap.remove(TopKMap.firstKey());
                }
            }
            catch(Exception e){
                System.out.println("Something has happened "+ e.toString());
            }

        } //~map

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Double k : TopKMap.keySet()) {
                context.write(new Text(k.toString()), TopKMap.get(k));
            }
        } //~cleanup

    } //~MapClass


    public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

        private static final TreeMap<Text, Text> TopKMap = new TreeMap <Text, Text>();

        @Override
        public void reduce (Text key, Iterable<Text> values, Context context)
                 throws IOException, InterruptedException {

            for (Text value : values) {
                TopKMap.put(new Text(key), new Text(value));
                if (TopKMap.size() > N) {
                    TopKMap.remove(TopKMap.firstKey()) ;
                }
            }


        } //~reduce

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text k : TopKMap.keySet()) {
                context.write(k, TopKMap.get(k));
            }
        } //~cleanup

    } // ReduceClass

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = new Job(conf, "TopTenSongs");
        job.setJarByClass(TopTenSongs.class);

        Path in = new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\input\\featuresdf.csv");
        Path out = new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\iseOPs");
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)?0:1);

        return 0;

    } //~run

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTenSongs(), args);

        System.exit(res);
    } //~main

} //~TopTenSongs