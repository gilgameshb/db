import org.bson.Document;

import com.mongodb.DB;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MainMongo {

	public static void main(String[] args) {
		//db.createUser({ user: "mongoadmin" , pwd: "mongoadmin", roles: ["userAdminAnyDatabase", "dbAdminAnyDatabase", "readWriteAnyDatabase"]})
	      
		//docker exec -it mymongo mongosh
		//docker run -d -p 27017:27017 --name mymongo mongo;docker exec -it mymongo mongosh

		
		MongoClient client = MongoClients.create("mongodb://127.0.0.1");
		MongoIterable<String> list = client.listDatabaseNames();
	      for (String name : list) {
	         System.out.println(name);
	      }
		MongoDatabase dbmongo = client.getDatabase("example");	
		MongoCollection<Document> gradesCollection = dbmongo.getCollection("grades");
		Document doc = new Document("playerName", "Ronaldo")
				.append("age", 25)
				.append("nationality", "Filipino")
				.append("JerseyNumber", 23)
				.append("position", "Guard");
		//gradesCollection.insertOne(doc);
		

	}

}
