package com.example.demo;

import com.example.demo.model.Person;
import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.util.Arrays;

@SpringBootApplication
public class BatchDemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchDemoApplication.class, args);
	}

	@Bean
	DataFactory dataFactory() {
		return new DataFactory();
	}

	@Bean
	CommandLineRunner commandLineRunner(DataFactory dataFactory) {

		Resource resource = new FileSystemResource("/Users/amarendra/IdeaProjects/batch-demo/src/main/resources/sample-data.csv");
		FlatFileItemWriter<Person> personFlatFileItemWriter = new FlatFileItemWriter<>();
		personFlatFileItemWriter.setResource(resource);
		return new CommandLineRunner() {
			@Override
			public void run(final String... strings) throws Exception {

				for (int i = 0; i < 10000; i++) {
					personFlatFileItemWriter.write(Arrays.asList(
							new Person(dataFactory.getFirstName(), dataFactory.getLastName())
					));
				}
			}
		};
	}
}
