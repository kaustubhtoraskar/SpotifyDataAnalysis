import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class HadoopConnect {
    public static class MongoMapper extends Mapper<Object, BasicDBObject, Text, Text> {
        public void map(Object key, BasicDBObject value, Context context) throws IOException, InterruptedException {
            String id = value.get("_id").toString();
            String name = value.get("name").toString();
            context.write(new Text(id), new Text(name));
        }
    }

    public static class MongoReduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text result = null;
            for(Text val : values) {
                result = val;
            }
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job");
        MongoConfigUtil.setInputURI(job.getConfiguration(), "mongodb://localhost:27017/mydb.student");
        job.setMapperClass(MongoMapper.class);
        job.setReducerClass(MongoReduce.class);
        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\kaustubh\\IdeaProjects\\bdaise\\mongoOP"));
        job.waitForCompletion(true);
    }
}


