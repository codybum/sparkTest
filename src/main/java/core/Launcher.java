package core;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class Launcher {


	public static void main(String[] args) throws Exception 
	{


	    /*
	    Configuration conf = HBaseConfiguration.create();
conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

Scan scan = new Scan();
scan.setCaching(100);

JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> hbaseRdd = hbaseContext.hbaseRDD(tableName, scan);

System.out.println("Number of Records found : " + hBaseRDD.count())
	     */

        String logFile = "test.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }


}
