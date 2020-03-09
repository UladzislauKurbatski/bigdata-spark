package lab3;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {

        File outputFile = new File("output-data");

        if (outputFile.exists()) {
            FileUtils.deleteDirectory(outputFile);
        }

        System.setProperty("hadoop.home.dir", "C:\\winutils");

        JavaSparkContext context = new JavaSparkContext(new SparkConf()
                .setMaster("local")
                .setAppName("spark"));

        JavaRDD<String> requestsInfo = context.textFile("000000");

        MapReducer mapReducer = new MapReducer();
        mapReducer.mapReduce(requestsInfo, "output-data");
        mapReducer.analyzeBrowsers(requestsInfo).forEach((key, value) -> System.out.println(String.format("%-20s : %d", key, value)));
    }
}
