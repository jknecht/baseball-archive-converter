import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertRecordCommand {
    private Connection conn;
    private Table table;
    private Row row;
    
    public InsertRecordCommand(Connection conn, Table table, Row row) {
        super();
        this.conn = conn;
        this.table = table;
        this.row = row;
    }

    public void execute() throws SQLException {
        InsertStatementBuilder insertStatementBuilder = new InsertStatementBuilder(table, row);
        String sql = insertStatementBuilder.build();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            System.out.println(sql);
            throw e;
        }
        int i = 1;
        for (String columnName : row.keySet()) {
            ps.setObject(i++, row.get(columnName), table.getColumn(columnName).getSQLType());
        }
        ps.executeUpdate();
        ps.close();
    }
}
