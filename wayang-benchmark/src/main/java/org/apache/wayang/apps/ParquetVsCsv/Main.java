package org.apache.wayang.apps.ParquetVsCsv;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.platform.JavaPlatform;
import java.util.LinkedList;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {

        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>");
                System.exit(1);
            }

            WayangContext wayangContext = new WayangContext();
            for (String platform : args[0].split(",")) {
                switch (platform) {
                    case "java":
                        wayangContext.register(Java.basicPlugin());
                        break;
                    default:
                        System.err.format("Unsuported platform: \"%s\"\n", platform);
                        System.exit(3);
                        return;
                }
            }

            JavaPlanBuilder parquetPlanBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("load_parquet")
                    .withUdfJarOf(Main.class);

            Path parquetPath = new Path("./lineorder.parquet");
            Path csvPath = new Path("./lineorder.csv");

            /* Start building the Apache WayangPlan */
            Collection<Tuple2<String, Integer>> parquetdata = parquetPlanBuilder
                    .readParquet(parquetPath).withName("Load parquet file")
                    .map((r) -> {
                        numRecords.getAndIncrement();
                        return r;
                    })
                    .map(r -> new Tuple2<>(r.get("LO_EXTENDEDPRIC").toString(), 1)).withName("Extract, add counter")
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                    .withName("Add counters")
                    .collect();

            JavaPlanBuilder csvPlanBuilder = new JavaPlanBuilder(wayangContext)
                    .withJobName("load__csv")
                    .withUdfJarOf(Main.class);
            Collection<Tuple2<String, Integer>> csvdata = csvPlanBuilder

                    .readTextFile(csvPath).withName("Load csv file")
                    .map((r) -> {
                        numRecords.getAndIncrement();
                        return r;
                    })
                    .map(r -> new Tuple2<>(r.get("LO_EXTENDEDPRIC").toString(), 1)).withName("Extract, add counter")
                    .reduceByKey(
                            Tuple2::getField0,
                            (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                    .withName("Add counters")
                    .collect();

        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

}
