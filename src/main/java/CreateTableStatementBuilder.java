import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Table;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class CreateTableStatementBuilder {
    Table table;

    public CreateTableStatementBuilder(Table table) {
        super();
        this.table = table;
    }

    public String build() throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("create table ").append(table.getName()).append(" (\n");
        List<? extends Column> columns = table.getColumns();
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
        return sb.toString();

    }
}
