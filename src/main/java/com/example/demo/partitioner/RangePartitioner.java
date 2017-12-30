package com.example.demo.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by amarendra on 30/12/17.
 */
public class RangePartitioner implements Partitioner {

    private JdbcTemplate jdbcTemplate;

    public RangePartitioner(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Map<String, ExecutionContext> partition(final int gridSize) {
        Map<String, ExecutionContext> result
                = new HashMap<>();

        final int[] count = {0};
        jdbcTemplate.query("SELECT count(*) FROM people", new RowCallbackHandler() {
            @Override
            public void processRow(final ResultSet resultSet) throws SQLException {
                int anInt = resultSet.getInt(1);
                System.out.println(anInt);
                count[0] = anInt+gridSize;
            }
        });

        int range = (count[0] /gridSize);
        int fromId = 1;
        int toId = range;

        for (int i = 1; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();

            System.out.println("\nStarting : Id" + i);
            System.out.println("fromId : " + fromId);
            System.out.println("toId : " + toId);

            context.putInt("fromId", fromId);
            context.putInt("toId", toId);

            // give each thread a name, thread 1,2,3
            context.putString("name", "Thread" + i);

            result.put("partition" + i, context);

            fromId = toId + 1;
            toId += range;

        }

        ExecutionContext context = new ExecutionContext();
        context.putInt("fromId", fromId);
        context.putInt("toId", count[0]);
        result.put("partition", context);

        return result;
    }
}
