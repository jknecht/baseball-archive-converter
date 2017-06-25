import com.healthmarketscience.jackcess.Table;

import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;

public class CreateTableStatementBuilder {
    Table table;

    public CreateTableStatementBuilder(Table table) {
        super();
        this.table = table;
    }

    public String build() throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb
            .append("create table ")
            .append(table.getName())
            .append(" (\n")
            .append(table.getColumns().stream()
                .map(column -> {
                    String typeString = "INTEGER";
                    try {
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
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    
                    if (Character.isDigit(column.getName().charAt(0))) {
                        return "\"" + column.getName() + "\" " + typeString; 
                    } else {
                        return column.getName() + " " + typeString;
                    }
                }).collect(Collectors.joining(",\n")))
            .append("\n);");
            
        return sb.toString();

    }
}
