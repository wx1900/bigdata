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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.BufferedReader;  
import java.io.FileInputStream;  
import java.io.InputStreamReader;  
import java.util.HashSet;  
import java.util.Set;
import java.util.Arrays;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit)context.getInputSplit();
			/** Get filename */
            String [] file_l = split.getPath().toString().split("/");
            String filename = file_l[file_l.length-1].toString();
            /** Filter input file, change to lowercase and remove non-letter and non-numeric */
			String str = value.toString();
            str = str.toLowerCase();
            char ch;
            for (int i = 0; i < str.length(); i++) {
                ch = str.charAt(i);
                if (ch >= '0' && ch <= '9') continue;
                if (ch >= 'a' && ch <= 'z') continue;
                str = str.replace(ch, ' ');
            }
			/** Split with space(' '), tab('\t'), enter('\r') and new line('\n') */
            StringTokenizer itr = new StringTokenizer(str);
            while(itr.hasMoreTokens()) {
                String token = itr.nextToken();
                keyInfo.set("<"+ token + "," + filename+">");
                valueInfo.set("1");
				context.write(keyInfo, valueInfo);
            }
			
            // BufferedReader stopWordFileBR = new BufferedReader(
            //     new InputStreamReader(new FileInputStream(new String("hdfs://localhost:9000/stop_words/stop_words_eng.txt"))));
            // Set<String> stopWordSet = new HashSet<String>();
            // String stopWord = null;
            // while ((stopWord = stopWordFileBR.readLine()) != null) {
            //     stopWordSet.add(stopWord);
            // }
            // if (stopWordFileBR != null) {
            //     stopWordFileBR.close();
            // }           
        }
        
    }
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        private Text keyInfo = new Text();
        public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			/** Cumulative sum of the value list */
            int sum = 0;
            for(Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
			/** input format <<token, filename>, [1,1,...]> */
			/** output format <token, <filename, sum>> */
            int splitIndex= key.toString().indexOf(",");
            info.set("<"+key.toString().substring(splitIndex + 1, key.toString().length()-1) + "," + sum+">");
            keyInfo.set(key.toString().substring(1, splitIndex));
            context.write(keyInfo, info);
        }
    }
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException {
			/** input format <token, [<filename, sum>,...]> */
            String fileList = new String(); /** add the value list */
            String total = new String(); /** add a total count */
            String[] file_list = {"", "", "", ""}; 
            int sum = 0;
            int count = 0;
            for(Text value : values) {
                int splitIndex = value.toString().indexOf(",");
                String [] tmp = value.toString().split(",");
                String filename = tmp[0].substring(1);
				/** remove stop words*/
                if (filename.equals("stop_words_eng.txt")) {
                    return;
                }
                file_list[count] = value.toString() + ";";
                count += 1;
                sum += Integer.parseInt(tmp[tmp.length-1].substring(0, tmp[tmp.length-1].length()-1));
            }
            Arrays.sort(file_list);
            for (int i = 0; i < file_list.length; i++) {
                fileList += file_list[i];
            }
            total = "<total,"+sum+">.";
            fileList += total;
            System.out.println(fileList);
            result.set(fileList);
            context.write(key, result);
        }
    }
    
    // public static String deleteStopWord(String str){
    //     try{
    //         BufferedReader stopWordFileBR = new BufferedReader(
    //             new InputStreamReader(new FileInputStream("/stop_words/stop_words_eng.txt")));
    //         Set<String> stopWordSet = new HashSet<String>();
    //         String stopWord = null;
    //         while ((stopWord = stopWordFileBR.readLine()) != null) {
    //             stopWordSet.add(stopWord);
    //         }
    //         if (stopWordFileBR != null) {
    //             stopWordFileBR.close();
    //         }
    //         String[] resultArray = str.split(" "); // split with ' '
    //         StringBuilder finalStr = new StringBuilder();
    //         for (int i = 0; i < resultArray.length; i++) {
    //             if (!stopWordSet.contains(resultArray[i])) {
    //                 finalStr = finalStr.append(resultArray[i].append(" "));
    //             }
    //         }
    //         return finalStr.toString();
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //         return "";
    //     }
    // }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}