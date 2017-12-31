package com.example.demo.config;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.task.configuration.DefaultTaskConfigurer;
import org.springframework.cloud.task.configuration.TaskConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * Created by amarendra on 30/12/17.
 */
@Configuration
public class AppConfig {

    @Bean
    @ConfigurationProperties("app.datasource")
    public DataSource appDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @Primary
    @Qualifier("dataSource")
    @ConfigurationProperties("spring.datasource")
    public DataSource dataSource(){
        return DataSourceBuilder.create().build();
    }


    @Bean
    public TaskConfigurer taskConfigurer(DataSource dataSource){
        return new DefaultTaskConfigurer(dataSource);
    }

    @Bean
    public BatchConfigurer batchConfigurer(@Qualifier("dataSource") DataSource dataSource){
        return new DefaultBatchConfigurer(dataSource);
    }
}
