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
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Index.ColumnDescriptor;
import com.healthmarketscience.jackcess.Table;


public class Main {

    public static final String SOURCE_FILE = "inbound/lahman2013_beta.mdb";
    public static final String TARGET_FILE = "outbound/lahman2013.sqlite";

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
		File inbound = new File(SOURCE_FILE);
		
		File outbound = new File(TARGET_FILE);
		if (outbound.exists()) {
			outbound.delete();
		}
		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + TARGET_FILE);
		
		
		Database db = Database.open(inbound);
		Set<String> tableNames = db.getTableNames();
		for (String tableName : tableNames) {
			StringBuilder sb = new StringBuilder();
			Table table = db.getTable(tableName);
			sb.append("create table ").append(table.getName()).append(" (\n");
			List<Column> columns = table.getColumns();
			int columnCounter = 0;
			for (Column column : columns) {
				String typeString = "INTEGER";
				int type = column.getSQLType();
				switch (type) {
				case Types.INTEGER:
				case Types.SMALLINT:
					typeString = "INTEGER";
					break;
				case Types.VARCHAR:
					typeString = "TEXT";
					break;
				case Types.DOUBLE:
					typeString = "REAL";
					break;
				case Types.TIMESTAMP:
					typeString = "NUMERIC";
					break;
				default:
					throw new RuntimeException("Unrecognized type: " + type);
				}
				if (columnCounter++ > 0) {
					sb.append(",\n");
				}
				if (Character.isDigit(column.getName().charAt(0))) {
					sb.append("\"");
				}
				sb.append(column.getName());
				if (Character.isDigit(column.getName().charAt(0))) {
					sb.append("\"");
				}
				sb.append(" ").append(typeString);
			}
			sb.append("\n);");

			
			System.out.println(sb.toString());
			System.out.println();
			conn.createStatement().execute(sb.toString());
			
			//insert the data
			Iterator<Map<String, Object>> rows = table.iterator();
			while(rows.hasNext()) {
				Map<String, Object> row = rows.next();
				StringBuilder sql = new StringBuilder();
				sql.append("insert into ").append(table.getName()).append(" (");
				Set<String> columnNames = row.keySet();
				columnCounter = 0;
				for (String columnName : columnNames) {
					if (columnCounter++ > 0) {
						sql.append(", ");
					}
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
					sb.append(columnName);
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
				}
			}
		}
		
		
		//insert the data
		for (String tableName : tableNames) {
			conn.setAutoCommit(false);
			Table table = db.getTable(tableName);
			System.out.println("Populating " + table.getRowCount() + " rows into " + table.getName());
			Iterator<Map<String, Object>> rows = table.iterator();
			while(rows.hasNext()) {
				Map<String, Object> row = rows.next();
				StringBuilder sql = new StringBuilder();
				sql.append("insert into ").append(table.getName()).append(" (");
				Set<String> columnNames = row.keySet();
				int columnCounter = 0;
				for (String columnName : columnNames) {
					if (columnCounter++ > 0) {
						sql.append(", ");
					}
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
					sql.append(columnName);
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
				}
				sql.append(") values (");
				
				columnCounter = 0;
				for (String columnName : columnNames) {
					if (columnCounter++ > 0) {
						sql.append(", ");
					}
					sql.append("?");
				}
				sql.append(")");
				
				PreparedStatement ps = null;
				try {
				ps = conn.prepareStatement(sql.toString());
			} catch (SQLException e) {
				System.out.println(sql.toString());
				throw e;
			}
				int i = 1;
				for (String columnName : columnNames) {
					ps.setObject(i++, row.get(columnName), table.getColumn(columnName).getSQLType());
				}
				ps.executeUpdate();
				ps.close();
			}
			
			conn.commit();
		}

		
		//index the tables
		//insert the data
		for (String tableName : tableNames) {
			conn.setAutoCommit(false);
			Table table = db.getTable(tableName);
			System.out.println("Indexing " + table.getName());
			List<Index> indexes = table.getIndexes();
			for(Index index : indexes) {
				
				StringBuilder sql = new StringBuilder();
				sql.append("create ");
				if (index.isUnique()) {
					sql.append("unique ");
				}
				sql.append("index ").append(tableName).append("_").append(index.getName()).append(" on ").append(tableName).append(" (");
				List<ColumnDescriptor> columns = index.getColumns();
				int columnCounter = 0;
				for (ColumnDescriptor column : columns) {
					if (columnCounter++ > 0) {
						sql.append(", ");
					}
					String columnName = column.getName();
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
					sql.append(columnName);
					if (Character.isDigit(columnName.charAt(0))) {
						sql.append("\"");
					}
				}
				sql.append(")");

                System.out.println(sql.toString());
                conn.createStatement().execute(sql.toString());
			}
			
			conn.commit();
		}

		conn.close();
		db.close();
	}
}
