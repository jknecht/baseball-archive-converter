import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AccessDatabase {
    private EventListener eventListener;
    private Database db;
    private Collection<Table> tables;
    
    public void open(String location) {
        try {
            this.db = DatabaseBuilder.open(new File(location));
            this.tables = db.getTableNames()
                    .stream()
                    .map(tableName -> {
                        try {
                            return db.getTable(tableName);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());        
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void registerEventListener(EventListener listener) {
        this.eventListener = listener;
    }
    
    public void iterateTables() {
        eventListener.startTables();
        this.tables.stream().forEach(table -> eventListener.onTable(table));
        eventListener.endTables();
    }
    
    public void iterateData() {
        this.tables.stream().forEach(table -> {
            eventListener.startRecords(table);
            StreamSupport.stream(table.spliterator(), false)
                .forEach(row -> this.eventListener.onRecord(table, row));
            eventListener.endRecords(table);
        });
    }
    
    public void iterateIndexes() {
        this.tables.stream().forEach(table -> {
            eventListener.startIndexes(table);
            table.getIndexes().stream()
                .forEach(index -> this.eventListener.onIndex(table, index));
            eventListener.endIndexes(table);
        });
    }
    
    public void close() {
        try {
            this.db.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
}
