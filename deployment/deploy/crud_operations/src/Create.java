import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;

public class Create {
    Connection postgresConnect;
    private MongoClient client;
    private MongoDatabase dbmongo;

    Create() {
        try {
            Class.forName("org.postgresql.Driver");
            setPostgresConnect(DriverManager.getConnection("jdbc:postgresql://postgres_db:5432/dvdrental", "postgres", "1234"));
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
        // Connect to mongoDB and get database
        setClient(MongoClients.create("mongodb://mongo:1234@mongo_db:27017"));
        setDbmongo(client.getDatabase("dvdrental"));

        etl();
    }

    private void etl() {
        // Initialize only when db is empty

            try {
                // Get all database metadata to get all tablenames to iterate through
                DatabaseMetaData dbmd = getPostgresConnect().getMetaData();
                try (ResultSet tables = dbmd.getTables(null, null, "%", new String[]{"TABLE"})) {
                    while (tables.next()) {
                        String tableName = tables.getString("TABLE_NAME");

                        // Get MongoCollection with tablename or create if not exist
                        MongoCollection<Document> selectedMongoCollection = getDbmongo().getCollection(tableName);

                        // Receive all table information and table Metadata
                        ResultSet rs;
                        Statement st = getPostgresConnect().createStatement();
                        rs = st.executeQuery("SELECT * FROM " + tableName);

                        ResultSetMetaData rsMetaData = rs.getMetaData();
                        int count = rsMetaData.getColumnCount();

                        System.out.println("########");
                        System.out.println("Tabelle: " + tableName);

                        // For every table entry write column name and value in a document
                        while (rs.next()) {
                            // here we create the doc object and in the for loop we just append the remaining column names and values
                            Document doc = new Document(rsMetaData.getColumnName(1), rs.getString(1));

                            for (int i = 1; i <= count; i++) {
                                String paramColum = rsMetaData.getColumnName(i);
                                String paramValue = rs.getString(i);

                                if (paramColum == null) {
                                    paramColum = "";
                                }
                                if (paramValue == null) {
                                    paramValue = "";
                                }
                                doc.append(paramColum, paramValue);
                            }
                            // Add document to the collection
                            selectedMongoCollection.insertOne(doc);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

    }

    /* Getter and Setter */
    public Connection getPostgresConnect() {
        return postgresConnect;
    }

    public void setPostgresConnect(Connection postgresConnect) {
        this.postgresConnect = postgresConnect;
    }

    public void setClient(MongoClient client) {
        this.client = client;
    }

    public MongoDatabase getDbmongo() {
        return dbmongo;
    }

    public void setDbmongo(MongoDatabase dbmongo) {
        this.dbmongo = dbmongo;
    }
}
