package com.gzunk.dbperformance;

import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@SpringBootApplication
public class ApplicationMain {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

//    @Bean
//    DataSource dataSource() {
//        PGSimpleDataSource ds = new PGSimpleDataSource();
//        ds.setServerName("postgres.dunblane");
//        ds.setDatabaseName("movies");
//        ds.setUser("postgres");
//        ds.setPassword("postgres");
//
//        return ds;
//    }

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

    public static void main(String[] args) {
        SpringApplication.run(ApplicationMain.class, args);
    }

    @PostConstruct
    public void runQuery() {

        JdbcOperations main = jdbcOperations();

        main.execute("CREATE TABLE TABLE1 (column_1 varchar(20), column_2 varchar(20))");
        main.execute("CREATE TABLE TABLE2 (column_1 varchar(20), column_2 varchar(20))");
        LOG.info("Create Tables");

        LOG.info("Count 1: {}", main.queryForObject("select count(*) from TABLE1", Integer.class));
        LOG.info("Count 2: {}", main.queryForObject("select count(*) from TABLE2", Integer.class));

        LOG.info("Populate Table 1");
        insertRandomData(100, main);

        LOG.info("Count 1: {}", main.queryForObject("select count (*) from table1", Integer.class));
        LOG.info("Count 2: {}", main.queryForObject("select count (*) from table2", Integer.class));

        main.query("SELECT column_1 FROM TABLE1", (ResultSet rs, int i) -> rs.getString(1)).forEach(LOG::info);

        LOG.info("Transferring Data");
        MyRowCallbackHandler callbackHandler = new MyRowCallbackHandler(jdbcOperations());
        main.query("select * from table1", callbackHandler);
        callbackHandler.saveResults();

        LOG.info("Count 1: {}", main.queryForObject("select count (*) from table1", Integer.class));
        LOG.info("Count 2: {}", main.queryForObject("select count (*) from table2", Integer.class));

    }

    private void clearData() {
        // jdbcOperations().update("DELETE FROM table1");
        jdbcOperations().update("DELETE FROM table2");
    }

    @SuppressWarnings("unused")
    private void copyData() {

        JdbcOperations jdbcOperations = jdbcOperations();

        LOG.info("Reading Data");
        List<Object[]> batchArgs = jdbcOperations.query("select * from table1", (rs, i) -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] results = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                results[j] = rs.getObject(j+1);
            }
            return results;
        });

        LOG.info("Writing Data");

        String columnPlaceholders = String.join(",", Collections.nCopies(batchArgs.get(0).length, "?"));
        jdbcOperations.batchUpdate("insert into table2 values (" + columnPlaceholders + ")", batchArgs);
    }

    private class MyRowCallbackHandler implements RowCallbackHandler {

        private static final int batchSize = 75;
        private List<Object[]> batchArgs;
        private JdbcOperations jdbcOperations;
        String columnPlaceholders;

        private MyRowCallbackHandler(JdbcOperations jdbcOperations) {
            this.jdbcOperations = jdbcOperations;
            batchArgs = new ArrayList<>();
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {

            int columnCount = rs.getMetaData().getColumnCount();
            Object[] results = new Object[columnCount];
            columnPlaceholders = String.join(",", Collections.nCopies(columnCount, "?"));

            for (int j = 0; j < columnCount; j++) {
                results[j] = rs.getObject(j+1);
            }
            batchArgs.add(results);

            if (batchArgs.size() >= batchSize ) {
                saveResults();
            }
        }

        public void saveResults() {
            LOG.info("Writing Data");
            jdbcOperations.batchUpdate("insert into table2 values (" + columnPlaceholders + ")", batchArgs);
            batchArgs = new ArrayList<>();
        }
    }

    @SuppressWarnings("unused")
    private void insertRandomData(final int count, JdbcOperations main) {

        Random rng = new Random();

        LOG.info("Inserting Data");
        for (int i = 0; i < count; i++) {
            Object[] args = {
                    generateString(rng),
                    generateString(rng),
            };

            // main.update("INSERT INTO table1 VALUES (?,?,?,?,?,?,?)", args);
            main.update("INSERT INTO TABLE1 VALUES (?,?)", args);
        }

        LOG.info("inserted {} rows", count);

    }

    private static Integer generateInt(Random rng) {
        return rng.nextInt(100);
    }

    private static String generateString(Random rng) {
        int length = 20;

        char[] text = new char[length];
        for (int i = 0; i < length; i++)
        {
            text[i] = ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
        }
        return new String(text);
    }
}
