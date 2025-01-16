package com.apachesparkjava.chapter03;

import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkFP {

    public static void main(String[] args) {
        try(final var spark =  SparkSession.builder()
                                        .appName("SparkFP")
                                        .master("local[*]")
                                        //.config("spark.scheduler.mode", "FAIR")
                                        .getOrCreate();
        final var sc = new JavaSparkContext(spark.sparkContext())) {
            final var data = Stream.iterate(1, n-> n + 1)
                                            .limit(5)
                                            .collect(Collectors.toList());
            data.forEach(System.out::println);
            final var myRDD = sc.parallelize(data);
            System.out.printf("RDD stg level: %s%n", myRDD.getStorageLevel());
            System.out.printf("Total elements in RDD: %d%n", myRDD.count());
            System.out.printf("Default number of partitions: %d%n", myRDD.partitions().size());

            final var max = myRDD.reduce(Integer::max);
            final var min = myRDD.reduce(Integer::min);
            final var sum = myRDD.reduce(Integer::sum);

            System.out.printf("Max: %d Min: %d Sum: %d%n", max, min, sum);
            System.out.println("File encoding: " + System.getProperty("file.encoding"));

            try(final var scanner = new Scanner(System.in)){
                System.out.println("Write something to end the program : ");
                scanner.nextLine();
            }

        }
    }
}
