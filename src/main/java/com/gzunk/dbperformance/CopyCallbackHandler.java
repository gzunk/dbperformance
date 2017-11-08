package com.gzunk.dbperformance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Component
public class CopyCallbackHandler implements RowCallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(CopyCallbackHandler.class);

    private List<Object[]> batchArgs;
    private String columnPlaceholders;

    @Value("${batch.size}")
    int batchSize;

    @Resource
    private JdbcOperations jdbcOperations;

    public CopyCallbackHandler() {
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

    void saveResults() {
        LOG.info("Writing Data");
        jdbcOperations.batchUpdate("insert into table2 values (" + columnPlaceholders + ")", batchArgs);
        batchArgs = new ArrayList<>();
    }
}
