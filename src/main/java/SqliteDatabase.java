import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;

public class SqliteDatabase implements EventListener {
    private Connection conn;
    
    public void open(String location) {
        try {
        File outbound = new File(location);
        if (outbound.exists()) {
            outbound.delete();
        }
        Class.forName("org.sqlite.JDBC");
        this.conn = DriverManager.getConnection("jdbc:sqlite:" + location);
        this.conn.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
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
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startRecords(Table table) {
        System.out.println("Table " + table.getName() + " has " + table.getRowCount() + " rows.");
    }

    @Override
    public void endRecords(Table table) {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public void startIndexes(Table table) {
        System.out.println("Creating indexes on table " + table.getName());
    }

    @Override
    public void endIndexes(Table table) {
        try {
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
