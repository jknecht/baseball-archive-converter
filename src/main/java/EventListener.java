import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public interface EventListener {
    void startTables();
    void onTable(Table table);
    void endTables();
    
    void startRecords(Table table);
    void onRecord(Table table, Row row);
    void endRecords(Table table);
    
    void startIndexes(Table table);
    void onIndex(Table table, Index index);
    void endIndexes(Table table);
}
