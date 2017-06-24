import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class AccessDatabase {
    private EventListener eventListener;
    private Database db;
    
    public void open(String location) {
        try {
            this.db = DatabaseBuilder.open(new File(location));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void registerEventListener(EventListener listener) {
        this.eventListener = listener;
    }
    
    public void iterateTables() {
        eventListener.startTables();
        try {
            Set<String> tableNames = db.getTableNames();
            for (String tableName : tableNames) {
                eventListener.onTable(db.getTable(tableName));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        eventListener.endTables();
    }
    
    public void iterateData() {
        try {
            for (String tableName : db.getTableNames()) {
                Table table = db.getTable(tableName);
                eventListener.startRecords(table);
                Iterator<Row> rows = table.iterator();
                while(rows.hasNext()) {
                    this.eventListener.onRecord(table, rows.next());
                }
                eventListener.endRecords(table);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
    }
    
    public void iterateIndexes() {
        try {
            for (String tableName : db.getTableNames()) {
                Table table = db.getTable(tableName);
                eventListener.startIndexes(table);
                List<? extends Index> indexes = table.getIndexes();
                for(Index index : indexes) {
                    this.eventListener.onIndex(table, index);
                }
                eventListener.endIndexes(table);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }        
        
    }
    
    public void close() {
        try {
            this.db.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
