import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Index;

import java.util.stream.Collectors;

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
        
        sql
            .append("index ")
            .append(tableName)
            .append("_")
            .append(index.getName())
            .append(" on ")
            .append(tableName)
            .append(" (")
            .append(index.getColumns().stream()
                .map(Index.Column::getName)
                .map(columnName -> {
                    if (Character.isDigit(columnName.charAt(0))) {
                        return "\"" + columnName + "\"";
                    } else {
                        return columnName;
                    }
                }).collect(Collectors.joining(", ")))
            .append(")");
        
        return sql.toString();
    }
}
