package core;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Launcher {

    //private static Gson gson;
    private static final Gson gson = new Gson();
	public static void main(String[] args) throws Exception
	{
        //gson = new GsonBuilder().create();
        //gson = new Gson();


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

        //String logFile = "test.txt"; // Should be some file on your system
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

        System.out.println("--------------------SET TABLES!");

        hbaseConf.set(TableInputFormat.INPUT_TABLE, "netflow");
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseConf);

        SQLContext sqlContext = new SQLContext(jsc);

        System.out.println("--------------------Creating hbase rdd!");
        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRdd = jsc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);



        // in the rowPairRDD the key is hbase's row key, The Row is the hbase's Row data
        JavaPairRDD<String, netFlow> rowPairRDD = javaPairRdd.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, netFlow>() {
                    @Override
                    public Tuple2<String, netFlow> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        //System.out.println("--------------------Getting ID!");
                        Result r = entry._2;
                        String keyRow = Bytes.toString(r.getRow());

                        //System.out.println("--------------------Define JavaBean!");
                        String json = new String(r.getValue(Bytes.toBytes("json"), Bytes.toBytes("data")));
                        //System.out.println("*" + json + "*");

                        netFlow flow = gson.fromJson(json, netFlow.class);

                        return new Tuple2<String, netFlow>(keyRow, flow);

                    }
                });

        // in the rowPairRDD the key is hbase's row key, The Row is the hbase's Row data
        /*
        JavaPairRDD<String, netASN> rowPairRDD = javaPairRdd.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, netASN>() {
                    @Override
                    public Tuple2<String, netASN> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        //System.out.println("--------------------Getting ID!");
                        Result r = entry._2;
                        String keyRow = Bytes.toString(r.getRow());

                        //System.out.println("--------------------Define JavaBean!");
                        netFlow flow = gson.fromJson(new String(r.getValue(Bytes.toBytes("json"), Bytes.toBytes("data"))), netFlow.class);
                        return new Tuple2<String, netASN>(keyRow, new netASN(flow.as_path, flow.as_dst, flow.as_src));
                    }
                });

*/
        System.out.println("--------------------COUNT RDD " + rowPairRDD.count());
        System.out.println("--------------------Create DataFrame!");
        //DataSet<Row> ssd = sqlContext.createDataset(rowPairRDD.values(), netFlow.class);
        DataFrame schemaRDD = sqlContext.createDataFrame(rowPairRDD.values(), netFlow.class);
        System.out.println("--------------------Loading Schema");
        schemaRDD.printSchema();
        System.out.println("--------------------COUNT Schema " + schemaRDD.count());

        schemaRDD.describe("as_path").show();
        schemaRDD.describe("as_dst").show();
        schemaRDD.describe("as_src").show();


        /*
        for (Tuple2<ImmutableBytesWritable, Result> test : javaPairRdd.take(10)) //or pairRdd.collect()
        {
            System.out.println(test._2);


            byte[] jbytes = test._2.getValue(Bytes.toBytes("json"), Bytes.toBytes("data"));
            netFlow flow = gson.fromJson(new String(jbytes), netFlow.class);
            String key = Bytes.toString(test._2.getRow());
            System.out.println("Key: " + key);
            System.out.println("Value: " + new String(jbytes));
        }
*/
       //Dataset<Row> peopleDF = spark.read().json("examples/src/main/resources/people.json");



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
