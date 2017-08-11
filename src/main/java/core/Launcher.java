package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;


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

        JavaSparkContext jsc = new JavaSparkContext(conf);

        Scan scan = new Scan();
        scan.setCaching(100);

        //JavaRDD<Tuple2<byte[], List<Tuple3<byte[], byte[], byte[]>>>> hbaseRdd = hbaseContext.hbaseRDD(tableName, scan);

        //System.out.println("Number of Records found : " + hBaseRDD.count())

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "bdm00.uky.edu");  // Here we are running zookeeper locally
        hbaseConf.set("hbase.cluster.distributed", "true");  // Here we are running zookeeper locally
        hbaseConf.set("hbase.rootdir", "hdfs://bdm00.uky.edu:8020/hbase");  // Here we are running zookeeper locally

        hbaseConf.set(TableInputFormat.INPUT_TABLE, "netflow");
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseConf);


        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRdd = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);

        for (Tuple2<ImmutableBytesWritable, Result> test : javaPairRdd.take(10)) //or pairRdd.collect()
        {
            System.out.println(test._1);
            System.out.println(test._2);
        }

        //List ls = javaPairRdd.collect();
        //javaPairRdd.

        jsc.stop();


        /*

        // Initialize hBase table if necessary
        HBaseAdmin admin = new HBaseAdmin(hbaseConf);
        if(!admin.isTableAvailable(args[1])) {
            HTableDescriptor tableDesc = new HTableDescriptor(args[1]);
            admin.createTable(tableDesc);
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                hbaseConf,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);
*/

        /*
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        */
    }


}
