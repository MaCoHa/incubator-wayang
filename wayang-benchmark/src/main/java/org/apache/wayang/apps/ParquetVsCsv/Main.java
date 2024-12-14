package org.apache.wayang.apps.ParquetVsCsv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.flink.core.fs.Path;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaParquetFileSource;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.platform.JavaPlatform;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {

        String filePath = "bechmarks.csv";
        String initialContent = "file_type,repetition,records_num,elasped_time_ns,elasped_time_ms\n";
        // file type : Parquet
        // repetition : x
        // records num: ParquetNumRecords
        // elasped time ns : <time>
        // elasped time ms : <time>

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(initialContent);
            writer.close();

        }

        System.out.println("File overwritten successfully.");

        Experiment experiment = new Experiment("parquet-bench-exp", new Subject("parquet-bench", "v0.1"));

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withExperiment(experiment)
                .withUdfJarOf(Main.class);

        // Step 2: Append to the file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            runcsvBenchmarks(5, writer, planBuilder);
            runParquetBenchmarks(5, writer, planBuilder);
            writer.close();
        }

        try {

        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

    private static void runParquetBenchmarks(int repetitions, BufferedWriter writer, JavaPlanBuilder parquetPlanBuilder)
            throws IOException, URISyntaxException {

        String filetype = "Parquet";

        for (int i = 0; i < repetitions; i++) {
            try {
                AtomicLong ParquetNumRecords = new AtomicLong();

                long startTime = System.nanoTime();

                String parquetPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv//lineorder.parquet";

                /* Start building the Apache WayangPlan */
                Collection<String> distinctLabels = parquetPlanBuilder
                        /* Read the text file */
                        .readParquetFile(new JavaParquetFileSource(new ParquetFileSource(parquetPath)))
                        .map(r -> r.getString(0))
                        .distinct()
                        .collect();
                long endTime = System.nanoTime();

                // Calculate elapsed time in nanoseconds
                long elapsedTime = endTime - startTime;

                // Convert to milliseconds (optional)
                double elapsedTimeInMs = elapsedTime / 1_000_000.0;

                // file type : Parquet
                // repetition : x
                // records num: ParquetNumRecords
                // elasped time ns : <time>
                // elasped time ms : <time>
                String benchmarkData = filetype + "," + i + "," + ParquetNumRecords.get() + "," + elapsedTime + ","
                        + elapsedTimeInMs;
                System.out.println(benchmarkData);

                writer.write(benchmarkData);

            } catch (Exception e) {
                System.err.println("App failed.");
                e.printStackTrace();
                System.exit(4);
            }
        }

    }

    private static void runcsvBenchmarks(int repetitions, BufferedWriter writer, JavaPlanBuilder csvPlanBuilder)
            throws IOException, URISyntaxException {

        String filetype = "CSV";

        for (int i = 0; i < repetitions; i++) {

            try {

                String csvPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv/lineorder.csv";
                AtomicLong CsvNumRecords = new AtomicLong();

                long startTime = System.nanoTime();

                Collection<Tuple2<String, Integer>> csvdata = csvPlanBuilder
                        .readTextFile(csvPath).withName("Load text file")
                        .filter(row -> !row.startsWith("lo_orderkey")).withName("Remove headers")
                        .withName("Remove headers")
                        .map((r) -> {
                            CsvNumRecords.getAndIncrement();
                            return r;
                        })
                        .map(line -> line.split(";"))
                        .map(r -> new Tuple2<>(r[16], 1)).withName("Extract, add counter")
                        .reduceByKey(
                                Tuple2::getField0,
                                (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                        .withName("Add counters")
                        .collect();

                long endTime = System.nanoTime();

                // Calculate elapsed time in nanoseconds
                long elapsedTime = endTime - startTime;

                // Convert to milliseconds (optional)
                double elapsedTimeInMs = elapsedTime / 1_000_000.0;

                // file type : Parquet
                // repetition : x
                // records num: ParquetNumRecords
                // elasped time ns : <time>
                // elasped time ms : <time>
                System.out.println(filetype);
                String benchmarkData = filetype + "," + i + "," + CsvNumRecords.get() + "," + elapsedTime + ","
                        + elapsedTimeInMs;
                System.out.println(benchmarkData);

                writer.write(benchmarkData);

            } catch (Exception e) {
                System.err.println("App failed.");
                e.printStackTrace();
                System.exit(4);
            }
        }

    }

}
