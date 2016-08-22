/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 *
 * @author zulk
 */
public class TestSaveToCassandra {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("aaaa")
                .set("spark.cassandra.connection.host", "192.168.100.105");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(10000));
        JavaDStream<String> stream = streamingContext.socketTextStream("192.168.100.105", 9999);
        stream.count().print();
        
        SQLContext sql = SQLContext.getOrCreate(streamingContext.sparkContext().sc());
        
        Dataset<Name> ds = sql.createDataset(ImmutableList.of(new Name("a","b")),Encoders.bean(Name.class));
        
        JavaDStream<Name> map = stream.map(s -> new Name(s, "e"));
        CassandraStreamingJavaUtil
                    .javaFunctions(map)
                    .writerBuilder("keyspace1", "name", CassandraJavaUtil.mapToRow(Name.class))
                    .saveToCassandra();
        
        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
