package com.example.demo.config;

import com.example.demo.listener.JobCompletionNotificationListener;
import com.example.demo.model.Person;
import com.example.demo.partitioner.RangePartitioner;
import com.example.demo.processor.PersonItemProcessor;
import com.example.demo.processor.PersonReverseItemProcessor;
import iso.std.iso._20022.tech.xsd.pain_007_001.AccountSchemeName1Choice;
import iso.std.iso._20022.tech.xsd.pain_007_001.ObjectFactory;
import iso.std.iso._20022.tech.xsd.pain_007_001.PersonIdentification5;
import org.fluttercode.datafactory.impl.DataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by amarendra on 29/12/17.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    public static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataFactory dataFactory;

    // tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[]{"firstName", "lastName"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    @StepScope
    public ItemReader<Person> databaseItemReader(@Qualifier("appDataSource") DataSource appDataSource) {
        JdbcCursorItemReader<Person> databaseReader = new JdbcCursorItemReader<>();

        databaseReader.setDataSource(appDataSource);
        databaseReader.setSql("SELECT first_name, last_name, id from people");
        databaseReader.setRowMapper(new BeanPropertyRowMapper<Person>() {
            @Override
            public Person mapRow(final ResultSet rs, final int rowNumber) throws SQLException {
                return new Person(rs.getString(1), rs.getString(2), rs.getInt(3));
            }
        });
        return databaseReader;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Person> pagingItemReader(
            @Value("#{stepExecutionContext[fromId]}") Integer fromId,
            @Value("#{stepExecutionContext[toId]}") Integer toId,
            @Qualifier("appDataSource") DataSource appDataSource) {
        System.out.println("Execution Context: fromId: " + fromId);
        System.out.println("Execution Context: toId: " + toId);
        JdbcPagingItemReader<Person> pagingItemReader = new JdbcPagingItemReader<>();

        pagingItemReader.setDataSource(appDataSource);
        pagingItemReader.setPageSize(10);

        PagingQueryProvider queryProvider = createQueryProvider(fromId, toId);
        pagingItemReader.setQueryProvider(queryProvider);

        pagingItemReader.setRowMapper(new BeanPropertyRowMapper<Person>() {
            @Override
            public Person mapRow(final ResultSet rs, final int rowNumber) throws SQLException {
                return new Person(rs.getString(1), rs.getString(2), rs.getInt(3));
            }
        });

        return pagingItemReader;
    }

    private PagingQueryProvider createQueryProvider(final Integer fromId, final Integer toId) {
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();

        queryProvider.setSelectClause("SELECT first_name, last_name, id");
        queryProvider.setFromClause("FROM people");
        queryProvider.setWhereClause("WHERE id <= " + toId + " and id >= " + fromId);
        queryProvider.setSortKeys(sortByIdAsc());

        return queryProvider;
    }

    private Map<String, Order> sortByIdAsc() {
        Map<String, Order> sortConfiguration = new HashMap<>();
        sortConfiguration.put("id", Order.ASCENDING);
        return sortConfiguration;
    }

    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    @Bean
    @StepScope
    public PersonReverseItemProcessor reverseProcessor() {
        return new PersonReverseItemProcessor();
    }


    @Bean
    @StepScope
    public FlatFileItemWriter<PersonIdentification5> personFileItemWriter(
            @Value("#{stepExecutionContext[fromId]}") Integer fromId,
            @Value("#{stepExecutionContext[toId]}") Integer toId) {
        FlatFileItemWriter<PersonIdentification5> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setResource(new FileSystemResource(new File(
                "/Users/amarendra/IdeaProjects/batch-demo/result/result"
                        + fromId + "_" + toId + ".csv")));
        flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(final Writer writer) throws IOException {
                writer.write("This is Header for the file" +
                        "\n The Person id start from: " + fromId);
            }
        });
        flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(final Writer writer) throws IOException {
                writer.write("\n The Person id end to: " + toId);
            }
        });
        flatFileItemWriter.setShouldDeleteIfEmpty(true);
        /*final DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        final BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"lastName", "firstName", "id"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        flatFileItemWriter.setLineAggregator(lineAggregator);*/
        flatFileItemWriter.setLineAggregator(new LineAggregator<PersonIdentification5>() {
            @Override
            public String aggregate(final PersonIdentification5 item) {
                return item.toString();
            }
        });

        return flatFileItemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(@Qualifier("appDataSource") DataSource appDataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(appDataSource);
        return writer;
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    //@Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Job exportUserJob(JobCompletionNotificationListener listener, Step masterStep) {
        return jobBuilderFactory.get("exportUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(masterStep)
                .end()
                .build();
    }

    @Bean
    public Step step1(ItemWriter writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean
    public Step masterStep(TaskExecutor taskExecutor,
                           Partitioner partitioner,
                           Step step2) {
        return stepBuilderFactory.get("masterStep")
                .partitioner("step2", partitioner)
                .gridSize(10)
                .step(step2)
                .taskExecutor(taskExecutor)
                .build();

    }

    @Bean
    public Step step2(PersonReverseItemProcessor reverseProcessor,
                      JdbcPagingItemReader pagingItemReader,
                      FlatFileItemWriter personFileItemWriter) {
        return stepBuilderFactory.get("step2")
                .<Person, Person>chunk(10)
                .reader(pagingItemReader)
                .processor(reverseProcessor)
                .writer(personFileItemWriter)
                .build();
    }

    // end::jobstep[]


    @Bean
    public Partitioner partitioner(@Qualifier("appDataSource") DataSource appDataSource) {
        final RangePartitioner rangePartitioner = new RangePartitioner(new JdbcTemplate(appDataSource));
        return rangePartitioner;
    }


    @Bean
    TaskExecutor taskExecutor() {
        final SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(10);
        return simpleAsyncTaskExecutor;
    }


}
