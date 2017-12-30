package com.example.demo.config;

import com.example.demo.listener.JobCompletionNotificationListener;
import com.example.demo.model.Person;
import com.example.demo.partitioner.RangePartitioner;
import com.example.demo.processor.PersonItemProcessor;
import com.example.demo.processor.PersonReverseItemProcessor;
import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.*;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.File;
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

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public DataSource dataSource;

    @Autowired
    public DataFactory dataFactory;

    // tag::readerwriterprocessor[]
    @Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames(new String[] { "firstName", "lastName" });
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                setTargetType(Person.class);
            }});
        }});
        return reader;
    }

    @Bean
    @StepScope
    public ItemReader<Person> databaseItemReader(DataSource dataSource) {
        JdbcCursorItemReader<Person> databaseReader = new JdbcCursorItemReader<>();

        databaseReader.setDataSource(dataSource);
        databaseReader.setSql("SELECT first_name, last_name, id from people");
        databaseReader.setRowMapper(new BeanPropertyRowMapper<Person>(){
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
            DataSource dataSource) {
        System.out.println("Execution Context: fromId: "+ fromId);
        System.out.println("Execution Context: toId: "+ toId);
        JdbcPagingItemReader<Person> pagingItemReader = new JdbcPagingItemReader<>();

        pagingItemReader.setDataSource(dataSource);
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
        queryProvider.setWhereClause("WHERE id <= "+toId+" and id >= "+ fromId);
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
    public FlatFileItemWriter<Person> personFileItemWriter(){
        FlatFileItemWriter<Person> flatFileItemWriter = new FlatFileItemWriter<>();
        final String fileName = dataFactory.getRandomWord();
        flatFileItemWriter.setResource(new FileSystemResource(new File(
                "/Users/amarendra/IdeaProjects/batch-demo/result/result"
                +fileName+".csv")));
        flatFileItemWriter.setShouldDeleteIfEmpty(true);
        final DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        final BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"lastName","firstName","id"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        flatFileItemWriter.setLineAggregator(lineAggregator);

        return flatFileItemWriter;
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
        writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
        writer.setDataSource(dataSource);
        return writer;
    }
    // end::readerwriterprocessor[]

    // tag::jobstep[]
    //@Bean
    public Job importUserJob(JobCompletionNotificationListener listener, DataSource dataSource, Step step1) {
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
                .<Person, Person> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean
    public Partitioner partitioner(JdbcTemplate jdbcTemplate) {
        final RangePartitioner rangePartitioner = new RangePartitioner(jdbcTemplate);
        rangePartitioner.partition(10);
        return rangePartitioner;
    }

    @Bean
    public Step masterStep(TaskExecutor taskExecutor,
                           Partitioner partitioner,
                           Step step2){
        return stepBuilderFactory.get("masterStep")
                .partitioner("step2", partitioner)
                .step(step2)
                .taskExecutor(taskExecutor)
                .build();

    }

    @Bean
    public Step step2(PersonReverseItemProcessor reverseProcessor,
                      JdbcPagingItemReader pagingItemReader,
                      TaskExecutor taskExecutor,
                      FlatFileItemWriter personFileItemWriter) {
        return stepBuilderFactory.get("step2")
                .<Person, Person> chunk(10)
                .reader(pagingItemReader)
                .processor(reverseProcessor)
                .writer(personFileItemWriter)
                .taskExecutor(taskExecutor)
                .throttleLimit(10)
                .build();
    }

    @Bean
    TaskExecutor taskExecutor() {
        final SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(10);
        return simpleAsyncTaskExecutor;
    }
    // end::jobstep[]

}
