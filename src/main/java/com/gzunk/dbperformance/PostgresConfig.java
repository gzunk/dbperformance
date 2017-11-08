package com.gzunk.dbperformance;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@Profile("Postgres")
public class PostgresConfig {

    @Bean
    DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName("postgres.dunblane");
        ds.setDatabaseName("dbperformance");
        ds.setUser("thomsona");
        ds.setPassword("postgres");
        return ds;
    }

    @Bean
    JdbcOperations jdbcOperations() {
        return new JdbcTemplate(dataSource());
    }
}
