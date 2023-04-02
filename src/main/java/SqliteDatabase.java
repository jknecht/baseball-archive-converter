import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

public class SqliteDatabase implements EventListener {
    private Connection conn;
    private Map<String, Integer> recordCounts;
    
    public void open(String location) {
        try {
        File outbound = new File(location);
        if (outbound.exists()) {
            outbound.delete();
        } else {
            // create the directory if necessary
            File dir = outbound.getParentFile();
            if (!dir.exists()) {
                dir.mkdirs();
            }
        }
        Class.forName("org.sqlite.JDBC");
        this.conn = DriverManager.getConnection("jdbc:sqlite:" + location);
        this.conn.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
        this.recordCounts = new TreeMap<>();
    }
    
    public void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTable(Table table) {
        try {
            System.out.println("Creating table " + table.getName());
            CreateTableStatementBuilder createTableStatementBuilder = new CreateTableStatementBuilder(table);
            String createTableStatement = createTableStatementBuilder.build();
            conn.createStatement().execute(createTableStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onRecord(Table table, Row row) {
        try {
            InsertRecordCommand insertRecordCommand = new InsertRecordCommand(conn, table, row);
            insertRecordCommand.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public void onIndex(Table table, Index index) {
        if (index.getName().startsWith(".")) return;
        try {
            CreateIndexStatementBuilder createIndexStatementBuilder = new CreateIndexStatementBuilder(table, index);
            String sql = createIndexStatementBuilder.build();
            conn.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startTables() {
    }

    @Override
    public void endTables() {
        commit();
    }

    @Override
    public void startRecords(Table table) {
        System.out.println("Table " + table.getName() + " has " + table.getRowCount() + " rows.");
        this.recordCounts.put(table.getName(), table.getRowCount());
    }

    @Override
    public void endRecords(Table table) {
        commit();        
    }

    @Override
    public void startIndexes(Table table) {
        System.out.println("Creating indexes on table " + table.getName());
    }

    @Override
    public void endIndexes(Table table) {
        commit();
    }
    
    private void commit() {
        try {
            this.conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void verify() {
        recordCounts.keySet().stream()
            .forEach(table -> verifyRecordCount(table));
    }
    
    private void verifyRecordCount(String table) {
        String sql = "select count(*) from " + table;
        try {
            ResultSet rs = conn.createStatement().executeQuery(sql);
            rs.next();
            int records = rs.getInt(1);
            int expected = recordCounts.get(table);
            if (records != expected) {
                throw new RuntimeException("Expected " + expected + " records in " + table + ".  Found " + records );
            } else {
                System.out.println("Verified " + table + " contains " + records + " records");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
    }
}
