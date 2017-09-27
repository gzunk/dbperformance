package com.gzunk.dbperformance;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SpringBootApplication
public class ApplicationMain {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationMain.class);
    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    @Bean
    DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName("postgres.dunblane");
        ds.setDatabaseName("movies");
        ds.setUser("postgres");
        ds.setPassword("postgres");

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
        LOG.info("Run a query");

        LOG.info("Count: {}", jdbcOperations().queryForObject("select count (*) from table2", Integer.class));
        clearData();
        LOG.info("Count: {}", jdbcOperations().queryForObject("select count (*) from table2", Integer.class));
        // insertRandomData(1000000);
        // selectData();

        copyDataBatched();
        // copyData();

        LOG.info("Count: {}", jdbcOperations().queryForObject("select count (*) from table2", Integer.class));

    }

    private void clearData() {
        // jdbcOperations().update("DELETE FROM table1");
        jdbcOperations().update("DELETE FROM table2");
    }

    private void selectData() {
        jdbcOperations().query("SELECT column_1 FROM table1", (ResultSet rs, int i) -> rs.getString(1)).forEach(LOG::info);
    }

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
        jdbcOperations.batchUpdate("insert into table2 values (?,?,?,?,?,?,?)", batchArgs);
    }

    private class MyRowCallbackHandler implements RowCallbackHandler {

        private static final int batchSize = 1000;
        private List<Object[]> batchArgs;
        private JdbcOperations jdbcOperations;

        public MyRowCallbackHandler(JdbcOperations jdbcOperations) {
            this.jdbcOperations = jdbcOperations;
            batchArgs = new ArrayList<>();
        }

        @Override
        public void processRow(ResultSet rs) throws SQLException {

            int columnCount = rs.getMetaData().getColumnCount();
            Object[] results = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                results[j] = rs.getObject(j+1);
            }
            batchArgs.add(results);

            if (batchArgs.size() >= batchSize || rs.isLast()) {
                LOG.info("Writing Data");
                jdbcOperations.batchUpdate("insert into table2 values (?,?,?,?,?,?,?)", batchArgs);
                batchArgs = new ArrayList<>();
            }
        }
    }

    private void copyDataBatched() {

        final int batchSize = 100;
        JdbcOperations jdbcOperations = jdbcOperations();

        LOG.info("Transferring Data");
        jdbcOperations.query("select * from table1", new MyRowCallbackHandler(jdbcOperations()));

    }

    private void insertRandomData(final int count) {

        Random rng = new Random();

        LOG.info("Inserting Data");

        JdbcOperations main = jdbcOperations();
        for (int i = 0; i < count; i++) {
            Object[] args = {
                    generateString(rng),
                    generateString(rng),
                    generateInt(rng),
                    generateString(rng),
                    generateString(rng),
                    generateInt(rng),
                    generateString(rng)
            };

            main.update("INSERT INTO table1 VALUES (?,?,?,?,?,?,?)", args);
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
