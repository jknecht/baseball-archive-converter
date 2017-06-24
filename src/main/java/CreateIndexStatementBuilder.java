import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Index;

import java.util.List;

public class CreateIndexStatementBuilder {
    private Table table;
    private Index index;
    
    public CreateIndexStatementBuilder(Table table, Index index) {
        super();
        this.table = table;
        this.index = index;
    }

    public String build() {
        StringBuilder sql = new StringBuilder();
        String tableName = table.getName();
        
        sql.append("create ");
        if (index.isUnique()) {
            sql.append("unique ");
        }
        sql.append("index ").append(tableName).append("_").append(index.getName()).append(" on ").append(tableName).append(" (");
        List<? extends Index.Column> columns = index.getColumns();
        int columnCounter = 0;
        for (Index.Column column : columns) {
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
        return sql.toString();
    }
}
