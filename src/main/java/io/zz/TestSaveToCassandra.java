/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 *
 * @author zulk
 */
public class TestSaveToCassandra {

    public static void main(String[] args) throws IOException, InterruptedException {
        SparkConf conf = new SparkConf().setAppName("aaaa")
                .setMaster("spark://192.168.100.105:7077")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.cassandra.connection.host", "192.168.100.105");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(10000));
        JavaDStream<String> stream = streamingContext.socketTextStream("192.168.100.105", 9999);

//        stream.count().print();
        SQLContext sql = SQLContext.getOrCreate(streamingContext.sparkContext().sc());

        Dataset<Name> ds = sql.createDataset(ImmutableList.of(new Name("a", "b")), Encoders.bean(Name.class));

        CassandraConnector cc = CassandraConnector.apply(conf);
        try (Session session = cc.openSession()) {
            String file = IOUtils.toString(TestSaveToCassandra.class.getResourceAsStream("/c.sql"));
            Arrays.stream(file.split(";"))
                    .map(s -> s.trim())
                    .filter(s -> !s.isEmpty())
                    .map(s -> s + ";")
                    .forEach((String str) -> session.execute(str));
        }

//        ds.toDF().write().mode(SaveMode.Overwrite).option("truncate", "true").jdbc("", "", new Properties());
        JavaDStream<Name> map = stream.map(s -> new Name(s, "e"));

        map.foreachRDD((s, t) -> process(s));

        CassandraStreamingJavaUtil
                .javaFunctions(map)
                .writerBuilder("keyspace1", "name", CassandraJavaUtil.mapToRow(Name.class))
                .saveToCassandra();

        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.stop();
    }

    private static void process(JavaRDD<Name> rdd) {
        if (!rdd.isEmpty()) {
            SQLContext sqlStream = SQLContext.getOrCreate(rdd.context());
            Dataset<Row> load = sqlStream
                    .read()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "keyspace1")
                    .option("table", "name")
                    .load();
            
            Dataset<Row> streamRdd = sqlStream.createDataFrame(rdd, Name.class);
            
            sqlStream.registerDataFrameAsTable(load, "c");
            sqlStream.registerDataFrameAsTable(streamRdd, "s");
            
            Dataset<Row> sql = sqlStream.sql("select * from c,s where c.name = s.name");
            sql.explain();
            sql.show();
            
            CassandraJavaUtil
                    .javaFunctions(rdd)
                    .joinWithCassandraTable("keyspace1", "name", CassandraJavaUtil.allColumns, CassandraJavaUtil.someColumns("name"), CassandraJavaUtil.mapRowTo(Name.class), CassandraJavaUtil.mapToRow(Name.class))
                    .take(10).forEach(s -> System.out.println(s._2));
        }
    }

}
