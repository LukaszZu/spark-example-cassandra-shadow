/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import java.io.IOException;
import org.apache.spark.SparkContext;
import org.apache.spark.launcher.SparkLauncher;

/**
 *
 * @author zulk
 */
public class Launcher {
    public static void main(String[] args) throws IOException, InterruptedException {
        SparkLauncher sparkLauncher = new SparkLauncher();
        Process spark = sparkLauncher
                .setAppName("APP NAME")
                .setSparkHome("/tmp")
                .setAppResource(SparkContext.jarOfClass(Launcher.class).get())
                .setMaster("spark://192.168.100.105:7077")
                .setMainClass("io.zz.TestSaveToCassandra")
                .launch();
        
                spark.waitFor();
    }
    
}
