import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SongsByArtistName {

    public static class TestMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            try {
                IntWritable pitchValue = new IntWritable();
                Configuration conf = context.getConfiguration();
                String name = conf.get("artist");
                String[] line = value.toString().split(",");
                int pitch = (int)Double.parseDouble(line[5]);
                pitchValue.set(pitch);
                if (line[2].equals(name))
                    context.write(new Text(line[1]),pitchValue );
            }
            catch (Exception e){
                System.out.println(e.toString());
            }
        }
    }

    public static class TestReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("artist","Ed Sheeran");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(SongsByArtistName.class);
        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestReducer.class);
        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\input\\featuresdf.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\SongsByArtistName"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
