package com.gzunk.dbperformance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Component
public class DatabaseRunner {

    private static final Logger LOG = LoggerFactory.getLogger(DatabaseRunner.class);

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    private static String generateString(Random rng) {
        int length = 20;

        char[] text = new char[length];
        for (int i = 0; i < length; i++) {
            text[i] = ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
        }
        return new String(text);
    }

    @SuppressWarnings("unused")
    private static Integer generateInt(Random rng) {
        return rng.nextInt(100);
    }

    @Value("${row.count}")
    private int rowCount;

    @Resource
    private JdbcOperations operations;

    private final CopyCallbackHandler callbackHandler;

    @Autowired
    public DatabaseRunner(CopyCallbackHandler callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    void runQuery() {

        LOG.info("Count 1: {}", operations.queryForObject("select count(*) from TABLE1", Integer.class));
        LOG.info("Count 2: {}", operations.queryForObject("select count(*) from TABLE2", Integer.class));

        LOG.info("Populate Table 1");
        insertRandomData(rowCount, operations);

        LOG.info("Count 1: {}", operations.queryForObject("select count (*) from table1", Integer.class));
        LOG.info("Count 2: {}", operations.queryForObject("select count (*) from table2", Integer.class));

        LOG.info("Transferring Data");
        operations.query("select * from table1", callbackHandler);
        callbackHandler.saveResults();

        LOG.info("Count 1: {}", operations.queryForObject("select count (*) from table1", Integer.class));
        LOG.info("Count 2: {}", operations.queryForObject("select count (*) from table2", Integer.class));
    }

    private int getNextFromSequence(JdbcOperations operations) {
        return operations.queryForObject("select nextval('column_1_sequence')", Integer.class);
    }

    private void insertRandomData(final int count, JdbcOperations main) {

        Random rng = new Random();
        List<Object[]> buffer = new ArrayList<>(count);

        LOG.info("Inserting Data");
        for (int i = 0; i < count; i++) {
            Object[] args = {
                    getNextFromSequence(main),
                    generateString(rng),
                    generateString(rng),
            };

            buffer.add(args);
        }

        main.batchUpdate("INSERT INTO TABLE1 VALUES (?,?,?)", buffer);
        LOG.info("inserted {} rows", count);
    }

    @SuppressWarnings("unused")
    private void copyData() {

        LOG.info("Reading Data");
        List<Object[]> batchArgs = operations.query("select * from table1", (rs, i) -> {
            int columnCount = rs.getMetaData().getColumnCount();
            Object[] results = new Object[columnCount];
            for (int j = 0; j < columnCount; j++) {
                results[j] = rs.getObject(j+1);
            }
            return results;
        });

        LOG.info("Writing Data");

        String columnPlaceholders = String.join(",", Collections.nCopies(batchArgs.get(0).length, "?"));
        operations.batchUpdate("insert into table2 values (" + columnPlaceholders + ")", batchArgs);
    }

    @SuppressWarnings("unused")
    public void clearData() {
        // jdbcOperations().update("DELETE FROM table1");
        operations.update("DELETE FROM table2");
    }
}
