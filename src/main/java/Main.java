
public class Main {

    public static final String SOURCE_FILE = "inbound/lahman2016.mdb";
    public static final String TARGET_FILE = "outbound/lahman2016.sqlite";

    public static void main(String[] args) {

        SqliteDatabase sqliteDatabase = new SqliteDatabase();
        sqliteDatabase.open(TARGET_FILE);

        AccessDatabase accessDatabase = new AccessDatabase();
        accessDatabase.registerEventListener(sqliteDatabase);
        accessDatabase.open(SOURCE_FILE);

        accessDatabase.iterateTables();
        accessDatabase.iterateData();
        accessDatabase.iterateIndexes();
        
        sqliteDatabase.verify();
        
        accessDatabase.close();
        sqliteDatabase.close();
        		
		System.out.println("fin.");
	}
}
