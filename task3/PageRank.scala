/** pack and submmit */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import scala.io._
import java.io.InputStreamReader
import java.io.BufferedReader
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

object PageRank {

  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)

    val hdfs = FileSystem.get(URI.create("hdfs://10.102.0.197:9000/Experiment_3/DataSet"), new Configuration)
    var fp : FSDataInputStream = hdfs.open(new Path("hdfs://10.102.0.197:9000/Experiment_3/DataSet"))
    var isr : InputStreamReader = new InputStreamReader(fp)
    var bReader : BufferedReader = new BufferedReader(isr)
    var line:String = bReader.readLine();

    var tot_links = List(("",List("a")));
    while(line!=null) {
      var tmpList = List((line.split("\t")(0), line.split("\t")(1).split(",").toList))
            tot_links = tot_links ++ tmpList
        line = bReader.readLine();
    }
    isr.close();
    bReader.close();

    tot_links = tot_links.drop(1);

    val links = sc.parallelize(tot_links).persist();
  
    var ranks = links.mapValues(v => 1.0); 
  
    for (i <- 0 until 10) {  
      val contributions=links.join(ranks).flatMap {  
        case (pageId,(links,rank)) => links.map(dest=>(dest,rank/links.size))  
      }  
      ranks=contributions.reduceByKey((x,y)=>x+y).mapValues(v=>0.15+0.85*v)  
    }

    val output = ranks.collect.sortWith{  
          case (a,b)=>{  
      a._2>b._2 //否则第三个字段降序  
          }  
        }
    // 结果存储在output中
    val outputPath = new Path("hdfs://10.102.0.197:9000/user/201500130058/Experiment_3_Spark");
    val outputWriter = new PrintWriter(hdfs.create(outputPath)) ;
    for(i<-0 to output.length-1){
        outputWriter.write("("+output(i)._1 + "," +"%11.10f".format(output(i)._2)+")\n");
    }
    outputWriter.close();
        sc.stop()
    }

}

