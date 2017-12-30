package com.example.demo;

import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

@SpringBootApplication
public class BatchDemoApplication {

	public static void main(String[] args) {
        Date startdate = new Date();
		SpringApplication.run(BatchDemoApplication.class, args);
		Date endDate = new Date();
        System.out.println("Time Taken --> "+ (endDate.getTime()-startdate.getTime()));
    }

	@Bean
	DataFactory dataFactory() {
		return new DataFactory();
	}

	CommandLineRunner commandLineRunner(DataFactory dataFactory) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/amarendra/IdeaProjects/batch-demo/src/main/resources/sample-data.csv", true));
		return new CommandLineRunner() {
			@Override
			public void run(final String... strings) throws Exception {
				for (int i = 0; i < 10000; i++) {
				    writer.write(dataFactory.getFirstName() + ","+dataFactory.getLastName());
				    writer.newLine();
				}
				writer.flush();
				writer.close();
			}
		};
	}
}
