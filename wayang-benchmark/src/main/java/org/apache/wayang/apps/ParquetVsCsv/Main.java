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
        String initialContent = "file_test,file_type,repetition,records_num,distict_values,elasped_time_ns,elasped_time_ms\n";
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


        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("run_parquet_vs_csv_bench")
                .withUdfJarOf(Main.class);

        // Step 2: Append to the file

        String parquetPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv//lineorder.parquet";
        String csvPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv/lineorder.csv";

        String parquet_smallPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv//lineorder_small.parquet";
        String csv_smallPath = "file:///opt/wayang/wayang-benchmark/src/main/java/org/apache/wayang/apps/ParquetVsCsv/lineorder_small.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            runcsvBenchmarks(5, writer, planBuilder, csvPath, "lineorder");
            runcsvBenchmarks(5, writer, planBuilder, csv_smallPath, "lineorder_small");

            runParquetBenchmarks(5, writer, planBuilder, parquetPath, "lineorder");
            runParquetBenchmarks(5, writer, planBuilder, parquet_smallPath, "lineorder_small");

            writer.close();
        }

        try {

        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

    private static void runParquetBenchmarks(int repetitions, BufferedWriter writer, JavaPlanBuilder parquetPlanBuilder,
            String parquetPath, String fileTest)
            throws IOException, URISyntaxException {

        String filetype = "Parquet";

        for (int i = 0; i < repetitions; i++) {
            try {
                AtomicLong ParquetNumRecords = new AtomicLong();

                long startTime = System.nanoTime();

                /* Start building the Apache WayangPlan */
                Collection<String> parquetData = parquetPlanBuilder
                        .readParquet(new JavaParquetFileSource(parquetPath)).withName("Load parquet file")
                        .map(r -> {
                            ParquetNumRecords.getAndIncrement();
                            // Access the desired field from the GenericRecord
                            String fieldValue = r.get("LO_ORDERKEY").toString(); 
                            return fieldValue.split(",")[0]; 
                        })
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
                String benchmarkData = 
                    fileTest + "," +
                    filetype + "," + 
                    i + "," + 
                    ParquetNumRecords.get() + ","+ 
                    parquetData.size() + ","+ 
                    elapsedTime + "," + 
                    elapsedTimeInMs + "\n";
                System.out.println(benchmarkData);

                writer.write(benchmarkData);

            } catch (Exception e) {
                System.err.println("App failed.");
                e.printStackTrace();
                System.exit(4);
            }
        }

    }

    private static void runcsvBenchmarks(int repetitions, BufferedWriter writer, JavaPlanBuilder csvPlanBuilder,
            String csvPath, String fileTest)
            throws IOException, URISyntaxException {

        String filetype = "CSV";

        for (int i = 0; i < repetitions; i++) {

            try {

                AtomicLong CsvNumRecords = new AtomicLong();

                long startTime = System.nanoTime();

                Collection<String> csvdata = csvPlanBuilder
                        .readTextFile(csvPath).withName("Load text file")
                        .filter(row -> !row.startsWith("LO_ORDERKEY")).withName("Remove headers")

                        .map(r -> {
                            CsvNumRecords.getAndIncrement();
                            return r.split(";")[0];
                        })
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
                String benchmarkData =
                    fileTest + "," +
                    filetype + "," + 
                    i + "," + 
                    CsvNumRecords.get() + ","+ 
                    csvdata.size() + ","+ 
                    elapsedTime + "," + 
                    elapsedTimeInMs + "\n";
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
