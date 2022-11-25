import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class Read {

	public static void main(String[] args) {

		//connect to mongodb
		MongoClient client = MongoClients.create("mongodb://127.0.0.1");

		// get database
		MongoDatabase dbmongo = client.getDatabase("example");

		//get collection
		MongoCollection<Document> collection = dbmongo.getCollection("film");
		

		
		System.out.println("Wie viele Filme?");
		System.out.println(collection.count());
		
		System.out.println("b.	Anzahl der unterschiedlichen Filme je ");
		collection = dbmongo.getCollection("inventory");
		//response Object
		Consumer<Document> processBlock = new Consumer<Document>() {
			@Override
			public void accept(Document document) {
				System.out.println(document);
			}
		};
		//build query
		List<? extends Bson> pipeline = Arrays.asList(
				new Document().append("$group",
						new Document().append("_id", new Document().append("store_id", "$store_id"))
								.append("COUNT(film_id)", new Document().append("$sum", 1))),
				new Document().append("$project", new Document().append("store_id", "$_id.store_id")
						.append("COUNT(film_id)", "$COUNT(film_id)").append("_id", 0)));
		
		//execute query with aggregate function
		collection.aggregate(pipeline).allowDiskUse(true).forEach(processBlock);

		System.out.println(
				"c.	Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert. ");
		collection = dbmongo.getCollection("film_actor");
		pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("film_actor", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "film_actor.actor_id")
                                .append("from", "actor")
                                .append("foreignField", "actor_id")
                                .append("as", "actor")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$actor")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("actor\u1390first_name", "$actor.first_name")
                                        .append("actor\u1390last_name", "$actor.last_name")
                                        .append("film_actor\u1390actor_id", "$film_actor.actor_id")
                                )
                                .append("COUNT(film_actor\u1390film_id)", new Document()
                                        .append("$sum", 1)
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("COUNT(film_actor.film_id)", "$COUNT(film_actor\u1390film_id)")
                                .append("film_actor.actor_id", "$_id.film_actor\u1390actor_id")
                                .append("actor.first_name", "$_id.actor\u1390first_name")
                                .append("actor.last_name", "$_id.actor\u1390last_name")
                                .append("_id", 0)
                        ), 
                new Document()
                        .append("$sort", new Document()
                                .append("COUNT(film_actor.film_id)", -1)
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("COUNT(film_actor\u1390film_id)", "$COUNT(film_actor.film_id)")
                                .append("film_actor.actor_id", "$film_actor.actor_id")
                                .append("actor.first_name", "$actor.first_name")
                                .append("actor.last_name", "$actor.last_name")
                        ), 
                new Document()
                        .append("$limit", 10)
        );
        
        collection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach(processBlock);
		  
		 

		System.out.println("d.	Die Erlöse je Mitarbeiter ");
		collection = dbmongo.getCollection("payment");

        pipeline = Arrays.asList(
                new Document()
                        .append("$addFields", new Document()
                                .append("amount_double", new Document()
                                        .append("$toDouble", "$amount")
                                )
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("staff_id", "$staff_id")
                                )
                                .append("SUM(amount_double)", new Document()
                                        .append("$sum", "$amount_double")
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("staff_id", "$_id.staff_id")
                                .append("SUM(amount_double)", "$SUM(amount_double)")
                                .append("_id", 0)
                        )
        );
        
        collection.aggregate(pipeline)
                .allowDiskUse(false)
                .forEach(processBlock);
		  
		 

		System.out.println("e.	Die IDs der 10 Kunden mit den meisten Entleihungen ");
		collection = dbmongo.getCollection("rental");

		pipeline = Arrays.asList(
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("customer_id", "$customer_id")
                                )
                                .append("COUNT(*)", new Document()
                                        .append("$sum", 1)
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("customer_id", "$_id.customer_id")
                                .append("COUNT(*)", "$COUNT(*)")
                                .append("_id", 0)
                        ), 
                new Document()
                        .append("$sort", new Document()
                                .append("COUNT(*)", -1)
                        ), 
                new Document()
                        .append("$limit", 10)
        );
        
        collection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach(processBlock);
		 

        System.out.println("f.	Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben");
        collection = dbmongo.getCollection("payment");
        pipeline = Arrays.asList(
                                		 new Document()
                                         .append("$addFields", new Document()
                                                 .append("amount_double", new Document()
                                                         .append("$toDouble", "$amount")
                                                 )
                                         ),
                                new Document()
                                        .append("$project", new Document()
                                                .append("_id", 0)
                                                .append("payment", "$$ROOT")
                                        ), 
                                new Document()
                                        .append("$lookup", new Document()
                                                .append("localField", "payment.customer_id")
                                                .append("from", "customer")
                                                .append("foreignField", "customer_id")
                                                .append("as", "customer")
                                        ), 
                                new Document()
                                        .append("$unwind", new Document()
                                                .append("path", "$customer")
                                                .append("preserveNullAndEmptyArrays", false)
                                        ), 
                                new Document()
                                        .append("$group", new Document()
                                                .append("_id", new Document()
                                                        .append("customer\u1390customer_id", "$customer.customer_id")
                                                        .append("customer\u1390last_name", "$customer.last_name")
                                                        .append("payment\u1390store", "$payment.store")
                                                        .append("customer\u1390first_name", "$customer.first_name")
                                                        .append("customer\u1390store_id", "$customer.store_id")
                                                )
                                                .append("SUM(payment\u1390amount_double)", new Document()
                                                        .append("$sum", "$payment.amount_double")
                                                )
                                        ), 
                                new Document()
                                        .append("$project", new Document()
                                                .append("customer.customer_id", "$_id.customer\u1390customer_id")
                                                .append("customer.first_name", "$_id.customer\u1390first_name")
                                                .append("customer.last_name", "$_id.customer\u1390last_name")
                                                .append("customer.store_id", "$_id.customer\u1390store_id")
                                                .append("SUM(payment.amount_double)", "$SUM(payment\u1390amount_double)")
                                                .append("_id", 0)
                                        ), 
                                new Document()
                                        .append("$sort", new Document()
                                                .append("SUM(payment.amount_double)", -1)
                                        ), 
                                new Document()
                                        .append("$project", new Document()
                                                .append("_id", 0)
                                                .append("customer.customer_id", "$customer.customer_id")
                                                .append("customer.first_name", "$customer.first_name")
                                                .append("customer.last_name", "$customer.last_name")
                                                .append("customer.store_id", "$customer.store_id")
                                                .append("SUM(payment\u1390amount_double)", "$SUM(payment.amount_double)")
                                        ), 
                                new Document()
                                        .append("$limit", 10)
                        );
        
        collection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach(processBlock);
        
        
        System.out.println("Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert ");
        collection = dbmongo.getCollection("rental");
        pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("rental", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "rental.inventory_id")
                                .append("from", "inventory")
                                .append("foreignField", "inventory_id")
                                .append("as", "inventory")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$inventory")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "inventory.film_id")
                                .append("from", "film")
                                .append("foreignField", "film_id")
                                .append("as", "film")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$film")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("film\u1390title", "$film.title")
                                        .append("inventory\u1390film_id", "$inventory.film_id")
                                )
                                .append("COUNT(*)", new Document()
                                        .append("$sum", 1)
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("inventory.film_id", "$_id.inventory\u1390film_id")
                                .append("film.title", "$_id.film\u1390title")
                                .append("COUNT(*)", "$COUNT(*)")
                                .append("_id", 0)
                        ), 
                new Document()
                        .append("$sort", new Document()
                                .append("COUNT(*)", -1)
                        ), 
                new Document()
                        .append("$limit", 10)
        );
        
        collection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach(processBlock);
        
        
        System.out.println("g.	Die 3 meistgesehenen Filmkategorien ");
        collection = dbmongo.getCollection("rental");
        pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("rental", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "rental.inventory_id")
                                .append("from", "inventory")
                                .append("foreignField", "inventory_id")
                                .append("as", "inventory")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$inventory")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "inventory.film_id")
                                .append("from", "film_category")
                                .append("foreignField", "film_id")
                                .append("as", "film_category")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$film_category")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$group", new Document()
                                .append("_id", new Document()
                                        .append("film_category\u1390category_id", "$film_category.category_id")
                                )
                                .append("COUNT(rental\u1390rental_id)", new Document()
                                        .append("$sum", 1)
                                )
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("film_category.category_id", "$_id.film_category\u1390category_id")
                                .append("COUNT(rental.rental_id)", "$COUNT(rental\u1390rental_id)")
                                .append("_id", 0)
                        ), 
                new Document()
                        .append("$sort", new Document()
                                .append("COUNT(rental.rental_id)", -1)
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("film_category.category_id", "$film_category.category_id")
                                .append("COUNT(rental\u1390rental_id)", "$COUNT(rental.rental_id)")
                        ), 
                new Document()
                        .append("$limit", 3)
        );
        
        
        
        collection.aggregate(pipeline)
                .allowDiskUse(true)
                .forEach(processBlock);
        
        System.out.println("View");
        
        pipeline = Arrays.asList(
                new Document()
                        .append("$project", new Document()
                                .append("_id", 0)
                                .append("cu", "$$ROOT")
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "cu.address_id")
                                .append("from", "address")
                                .append("foreignField", "address_id")
                                .append("as", "a")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$a")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "a.city_id")
                                .append("from", "city")
                                .append("foreignField", "city_id")
                                .append("as", "city")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$city")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$lookup", new Document()
                                .append("localField", "city.country_id")
                                .append("from", "country")
                                .append("foreignField", "country_id")
                                .append("as", "country")
                        ), 
                new Document()
                        .append("$unwind", new Document()
                                .append("path", "$country")
                                .append("preserveNullAndEmptyArrays", false)
                        ), 
                new Document()
                        .append("$project", new Document()
                                .append("cu.customer_id", "$cu.customer_id")
                                .append("a.address", "$a.address")
                                .append("a.zip code", "$a.postal_code")
                                .append("a.phone", "$a.phone")
                                .append("cu.fullname", new Document()
                                        .append("$concat", Arrays.asList(
                                                "$cu.first_name",
                                                " ",
                                                "$cu.last_name"
                                            )
                                        )
                                )
                                .append("city.city", "$city.city")
                                .append("country.country", "$country.country")
                                .append("cu.sid", "$cu.store_id")
                                .append("note", new Document()
                                        .append("$cond", new Document()
                                                .append("if", new Document()
                                                        .append("$eq", Arrays.asList(
                                                                "$cu.activebool",
                                                                "t"
                                                            )
                                                        )
                                                )
                                                .append("then", "active")
                                                .append("else", "")
                                        )
                                )
                                .append("_id", 0)
                        )
        );
        
        
        dbmongo.createView ("customer_list","customer", pipeline);
        
        collection.aggregate(pipeline)
                .allowDiskUse(false)
                .forEach(processBlock);
        
        
        
	}

}
