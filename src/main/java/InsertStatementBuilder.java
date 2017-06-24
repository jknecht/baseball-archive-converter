import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Row;

import java.util.Set;

public class InsertStatementBuilder {
    private Table table;
    private Row row;
    
    public InsertStatementBuilder(Table table, Row row) {
        super();
        this.table = table;
        this.row = row;
    }

    public String build() {
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
        return sql.toString();
    }
}
