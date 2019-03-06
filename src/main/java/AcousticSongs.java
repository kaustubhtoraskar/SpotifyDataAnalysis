import java.util.*;

import java.io.IOException;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AcousticSongs {

    //Mapper class
    public static class E_EMapper extends MapReduceBase implements
            Mapper<LongWritable ,/*Input key Type */
                    Text,                /*Input value Type*/
                    Text,                /*Output key Type*/
                    DoubleWritable>        /*Output value Type*/
    {
        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, DoubleWritable> output,

                        Reporter reporter) throws IOException {
            try{
            String[] line = value.toString().split(",");
            String songName = line[1];
            Double Acousticness = Double.parseDouble(line[9]);
            output.collect(new Text(songName), new DoubleWritable(Acousticness));
        }
            catch (Exception e){
                System.out.println(e.toString());
            }
        }

    }

    //Reducer class
    public static class E_EReduce extends MapReduceBase implements Reducer< Text, DoubleWritable, Text, DoubleWritable > {

        //Reduce function
        public void reduce( Text key, Iterator <DoubleWritable> values,
                            OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double minAcousticness = 0.5;
            double val =0;
            while (values.hasNext()) {
//                System.out.println(values.next().get());
                if((val = values.next().get()) > minAcousticness) {
                    output.collect(key, new DoubleWritable(val));
                    System.out.println(val +""+ key);
                }
            }
        }
    }

    //Main function
    public static void main(String args[])throws Exception {
        JobConf conf = new JobConf(AcousticSongs.class);

        conf.setJobName("acousticness");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
        conf.setMapperClass(E_EMapper.class);
        conf.setCombinerClass(E_EReduce.class);
        conf.setReducerClass(E_EReduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\input.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\accousticSongs"));

        JobClient.runJob(conf);
    }
}
