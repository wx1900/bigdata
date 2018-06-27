import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import java.net.URI;
import java.io.BufferedReader;  
import java.io.FileInputStream; 
import java.io.InputStreamReader;  
import java.math.RoundingMode;
import java.util.HashSet;  
import java.util.Set;
import java.text.DecimalFormat;
import java.util.Arrays;

public class pageRank {
    static Configuration conf = new Configuration();

    public static void deleteDirectoryOnHDFS(String deletePath)
            throws IOException {
        // 读取HDFS上的文件系统
        FileSystem hdfs = FileSystem.get(URI.create(deletePath), conf);
        Path deleteDir = new Path(deletePath);
        // 在HDFS上删除文件夹
        hdfs.deleteOnExit(deleteDir);
        // 释放资源
        hdfs.close();
        System.out.println("删除文件夹成功");
    }

    public static class sortMapper extends Mapper <LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String srcUrl = line.split("\t")[0];
            String rest = line.split("\t")[1];
            context.write(new Text(srcUrl), new Text(rest));
        }
    }
    
    public static class sortReducer extends Reducer <Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            String urls = "";
            String pr = "1"; // initial pr = 1
            for (Text value: values) {
                if (value.toString().split(",").length > 1) {
                    urls = value.toString();
                } else {
                    pr = value.toString();
                }
            }
            String result = pr + "\t" + urls;
            context.write(key, new Text(result));
        }
    }

    public static class pageRankMapper extends Mapper <LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String srcUrl = line.split("\t")[0];
            double pr = Double.valueOf(line.split("\t")[1]);
            String urls[] = line.split("\t")[2].split(",");
            int urlSize = urls.length;
            double score = pr / urlSize;
            for (int i = 0; i < urlSize; i++) {
                context.write(new Text(urls[i]), new DoubleWritable(score));
            }
            context.write(new Text(srcUrl), new DoubleWritable(-pr));
        }
    }

    public static class pageRankReducer extends Reducer <Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce (Text key, Iterable<DoubleWritable> values, Context context) 
                    throws IOException, InterruptedException {
            double tot = 0.0;
            double pr = 0.0;
            double dampFactor = 0.85;
            for (DoubleWritable value: values) {
                double val = value.get();
                if (val < 0) { pr = -val; }
                else { tot += val; }
            }
            pr = (Double) (1.0 - dampFactor) + dampFactor * tot;
            context.write(key, new DoubleWritable(pr));
        }
    }

    public static class finReducer extends Reducer <Text, DoubleWritable, Text, Text> {
         public void reduce (Text key, Iterable<DoubleWritable> values, Context context) 
                    throws IOException, InterruptedException {
            double tot = 0.0;
            double pr = 0.0;
            double dampFactor = 0.85;
            for (DoubleWritable value: values) {
                double val = value.get();
                if (val < 0) { pr = -val; }
                else { tot += val; }
            }
            pr = (double) (1.0 - dampFactor) + dampFactor * tot;
            String result = "(" + key.toString() + "," + pr + ")";
            context.write(new Text(result), new Text(""));
        }
    }
    public static class finsortMapper extends Mapper <LongWritable, Text, DoubleWritable, Text> {
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString();
            String srcUrl = line.split("\t")[0];
            // srcUrl = srcUrl.substring(1);
            String pr = line.split("\t")[1];
            // pr = pr.substring(0, pr.length()-2);
            context.write(new DoubleWritable(-Double.valueOf(pr)), new Text(srcUrl));
        }
    }

    public static class finsortReducer extends Reducer <DoubleWritable, Text, NullWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            for (Text value: values) {
                Double pr = -Double.valueOf(key.toString());
                // pr = Double.valueOf(String.format("%.11f", pr));
                // DecimalFormat df = new DecimalFormat("0.0000000000");
                // df.setRoundingMode(RoundingMode.HALF_DOWN); 
                // pr = Double.valueOf(df.format(pr));
                String result = "(" + value.toString() + "," + String.format("%.10f", pr) + ")"; // 保留最后的0
                context.write(null, new Text(result));
            }
        }
    }

    public static void main (String[] args) throws Exception {
        // Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
         conf.set("mapred.textoutputformat.separator", "\t"); 
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        String pathIn = "/user/201500130058/task3/input/*";
        String pathOut = "/user/201500130058/task3/output";
        String pathSort = "/user/201500130058/task3/sort";
        String pathMid = "/user/201500130058/task3/input/mid";
        String hdfsPath = "hdfs://localhost:9000";
        conf.set("fs.default.name", hdfsPath);
        
        int itr_max = 10;
        for (int i = 0; i < itr_max; i++) {
            Job job = new Job(conf, "pageRank");
            job.setJarByClass(pageRank.class);
            job.setMapperClass(sortMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(sortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // if (i > 0) {
            deleteDirectoryOnHDFS(pathSort);
            // }
            FileInputFormat.addInputPath(job, new Path(pathIn));            
            FileOutputFormat.setOutputPath(job, new Path(pathSort));
            
            job.waitForCompletion(true);

            Job job1 = new Job(conf, "pageRank");
            job1.setJarByClass(pageRank.class);
            job1.setMapperClass(pageRankMapper.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(DoubleWritable.class);
            // if (i < itr_max-1) {
            job1.setReducerClass(pageRankReducer.class);
            // } else {
                // job1.setReducerClass(finReducer.class);
            // }
            
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job1, new Path(pathSort));

            // if (i > 0) {
            deleteDirectoryOnHDFS("hdfs://localhost:9000/user/201500130058/task3/input/mid");
            // }
            // if (i < itr_max-1) {
            FileOutputFormat.setOutputPath(job1, new Path(pathMid));
            // } else {
                // FileOutputFormat.setOutputPath(job1, new Path(pathOut));
            // }
            
            job1.waitForCompletion(true);
        }
        Job job = new Job(conf, "pageRank");
        job.setJarByClass(pageRank.class);
        job.setMapperClass(finsortMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(finsortReducer.class);
        job.setOutputKeyClass(NullWritable.class); // remove jiangefu
        job.setOutputValueClass(Text.class);
         
        conf.set("mapred.textoutputformat.separator", ""); 
        deleteDirectoryOnHDFS("hdfs://localhost:9000/user/20150013058/task3/output");
        FileInputFormat.addInputPath(job, new Path(pathMid));            
        FileOutputFormat.setOutputPath(job, new Path(pathOut));
        
        job.waitForCompletion(true);
    }
}   