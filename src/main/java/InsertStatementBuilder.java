import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Row;

import java.util.stream.Collectors;

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
        sql
            .append("insert into ")
            .append(table.getName())
            .append(" (")
            .append(row.keySet().stream()
                    .map(columnName -> {
                        if (Character.isDigit(columnName.charAt(0))) {
                            return "\"" + columnName + "\"";
                        } else {
                            return columnName;
                        }
                    })
                    .collect(Collectors.joining(", ")))
            .append(") values (")
            .append(row.keySet().stream()
                    .map(columnName -> "?").collect(Collectors.joining(", ")))
            .append(")");
        
        return sql.toString();
    }
}
