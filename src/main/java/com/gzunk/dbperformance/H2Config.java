package com.gzunk.dbperformance;

import org.h2.jdbcx.JdbcDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Configuration
@Profile("H2")
public class H2Config {

    private static final Logger LOG = LoggerFactory.getLogger(H2Config.class);

    @Bean
    DataSource dataSource() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        ds.setUser("alasdair");
        ds.setPassword("thomson");
        return ds;
    }

    @Bean
    JdbcOperations jdbcOperations() {
        return new JdbcTemplate(dataSource());
    }

    @PostConstruct
    public void createTables() {
        JdbcOperations operations = jdbcOperations();

        LOG.info("Create Tables");
        operations .execute("CREATE TABLE TABLE1 (column_1 numeric, column_2 varchar(20), column_3 varchar(20))");
        operations .execute("CREATE TABLE TABLE2 (column_1 numeric, column_2 varchar(20), column_3 varchar(20))");
        operations .execute("CREATE SEQUENCE column_1_sequence START WITH 1");
    }
}
