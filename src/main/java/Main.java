
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;


public class Main {

    public static final String SOURCE_FILE = "inbound/lahman2016.mdb";
    public static final String TARGET_FILE = "outbound/lahman2016.sqlite";

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		File inbound = new File(SOURCE_FILE);
		
		File outbound = new File(TARGET_FILE);
		if (outbound.exists()) {
			outbound.delete();
		}
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + TARGET_FILE);
		
		
		Database db = DatabaseBuilder.open(inbound);
		Set<String> tableNames = db.getTableNames();
		for (String tableName : tableNames) {
		    Table table = db.getTable(tableName);
		    CreateTableStatementBuilder createTableStatementBuilder = new CreateTableStatementBuilder(table);
		    String createTableStatement = createTableStatementBuilder.build();
			
			System.out.println(createTableStatement);
			System.out.println();
			conn.createStatement().execute(createTableStatement);			
		}
		
		
		//insert the data
		for (String tableName : tableNames) {
			conn.setAutoCommit(false);
			Table table = db.getTable(tableName);
			System.out.println("Populating " + table.getRowCount() + " rows into " + table.getName());
			Iterator<Row> rows = table.iterator();
			while(rows.hasNext()) {
				Row row = rows.next();
				InsertRecordCommand insertRecordCommand = new InsertRecordCommand(conn, table, row);
				insertRecordCommand.execute();
			}
			conn.commit();
		}

		
		//index the tables
		for (String tableName : tableNames) {
			conn.setAutoCommit(false);
			Table table = db.getTable(tableName);
			System.out.println("Indexing " + table.getName());
			List<? extends Index> indexes = table.getIndexes();
			for(Index index : indexes) {
				CreateIndexStatementBuilder createIndexStatementBuilder = new CreateIndexStatementBuilder(table, index);
				String sql = createIndexStatementBuilder.build();

                System.out.println(sql);
                conn.createStatement().execute(sql);
			}
			
			conn.commit();
		}

		conn.close();
		db.close();
		
		System.out.println("fin.");
	}
}
