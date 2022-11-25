import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Main {
//http://localhost:8080/?pgsql=postgresdb&username=postgres&db=dvdrental&ns=public
	// https://www.youtube.com/watch?v=uklyCSKQ1Po
	// mongodbs

// all views select viewname from pg_catalog.pg_views;
	public static void main(String[] args) {
		Connection c = null;
		try {
			//get driver
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dvdrental", "postgres", "1234");

			//connect to mongo 
			MongoClient client = MongoClients.create("mongodb://127.0.0.1");
			
			//get database with the name example
			MongoDatabase dbmongo = client.getDatabase("example");	

			
			//get all database metadata to get all tablenames to iterate through
			DatabaseMetaData dbmd = c.getMetaData();
			try (ResultSet tables = dbmd.getTables(null, null, "%", new String[] { "TABLE" })) {
				while (tables.next()) {
					
					
					String tableName = tables.getString("TABLE_NAME");
					//get MongoCollection with tablename or create if not exist
					MongoCollection<Document> selectedMongoCollection = dbmongo.getCollection(tableName);

					Statement st = c.createStatement();
					//recieve all table information and table Metadata
					ResultSet rs = st.executeQuery("SELECT * FROM "+tableName);
					ResultSetMetaData rsMetaData = rs.getMetaData();
					int count = rsMetaData.getColumnCount();
					System.out.println("########");
					System.out.println("Tabelle: "+tableName);
					while (rs.next()) {
						//for every entry in the table write columnname and value in a Document
						// here we create the doc object and in the for loop we just append the remaining column names and values
						Document doc = new Document(rsMetaData.getColumnName(1),rs.getString(1));

						for(int i = 1; i<=count; i++) {
							String paramColum = rsMetaData.getColumnName(i);
							String paramValue = rs.getString(i);
							
							if(paramColum==null) {
								paramColum="";
							}
							if(paramValue==null) {
								paramValue="";
							}
					  		doc.append(paramColum, paramValue);

					      }
						System.out.println(doc.toJson());
						//add the document to the collection
						selectedMongoCollection.insertOne(doc);
					}
					
				
					
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");
	}

}
