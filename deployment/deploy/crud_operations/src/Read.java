import com.mongodb.client.*;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

public class Read {
    private MongoClient client;
    private MongoDatabase dbmongo;

    public Read() {
        // Connect to mongoDB and get database
        setClient(MongoClients.create("mongodb://mongo:1234@mongo_db:27017"));
        setDbmongo(client.getDatabase("dvdrental"));

        read();
    }

    private void read() {
        System.out.println("a. Gesamtanzahl der verfügbaren Filme");
        System.out.println("--> Query: Read.java, Z. 28-70");

        MongoCollection<Document> collection = getDbmongo().getCollection("inventory");
        System.out.println("    " + collection.countDocuments());

        System.out.println("b. Anzahl der unterschiedlichen Filme je Standort");
        System.out.println("--> Query: Read.java, Z. 34-53");

        collection = dbmongo.getCollection("inventory");
        Consumer<Document> processBlock = document -> System.out.println("    " + document.toJson());

        List<? extends Bson> pipeline = Arrays.asList(
                new Document().append("$project",
                        new Document().append("film_id", "$film_id").append("store_id", "$store_id").append("_id", 0)),
                new Document().append("$group",
                        new Document().append("_id", new BsonNull()).append("distinct",
                                new Document().append("$addToSet", "$$ROOT"))),
                new Document().append("$unwind",
                        new Document().append("path", "$distinct").append("preserveNullAndEmptyArrays", false)),
                new Document().append("$replaceRoot", new Document().append("newRoot", "$distinct")));

        // execute query with aggregate function
        AggregateIterable<Document> test = collection.aggregate(pipeline);
        MongoCursor<Document> iterator = test.iterator();
        HashMap<String, Integer> filmCounterPerLocation = new HashMap<String, Integer>();

        while (iterator.hasNext()) {
            Document next = iterator.next();
            String film = next.get("film_id").toString();
            String store = next.get("store_id").toString();
            if (filmCounterPerLocation.containsKey(store)) {
                int counter = filmCounterPerLocation.get(store);
                counter++;
                filmCounterPerLocation.put(store, filmCounterPerLocation.get(store) + 1);

            } else {
                filmCounterPerLocation.put(store, 1);

            }

        }
        for (String key : filmCounterPerLocation.keySet()) {
            int counter = filmCounterPerLocation.get(key);
            System.out.println("Store ID :" + key);
            System.out.println("Filme :" + counter);
        }

        System.out.println("c. Die Vor- und Nachnamen der 10 Schauspieler mit den meisten Filmen, absteigend sortiert");
        System.out.println("--> Query: Read.java, Z. 75-114");

        collection = getDbmongo().getCollection("film_actor");
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

        System.out.println("d. Die Erlöse je Mitarbeiter");
        System.out.println("--> Query: Read.java, Z. 136-165");

        collection = getDbmongo().getCollection("payment");

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

        System.out.println("e. Die IDs der 10 Kunden mit den meisten Entleihungen");
        System.out.println("--> Query: Read.java, Z. 169-197");

        collection = getDbmongo().getCollection("rental");

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

        System.out.println("f. Die Vor- und Nachnamen sowie die Niederlassung der 10 Kunden, die das meiste Geld ausgegeben haben");
        System.out.println("--> Query: Read.java, Z. 202-268");

        collection = getDbmongo().getCollection("payment");
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

        System.out.println("g. Die 10 meistgesehenen Filme unter Angabe des Titels, absteigend sortiert");
        System.out.println("--> Query: Read.java, Z. 273-332");

        collection = getDbmongo().getCollection("rental");

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

        System.out.println("h. Die 3 meistgesehenen Filmkategorien");
        System.out.println("--> Query: Read.java, Z. 337-400");

        collection = getDbmongo().getCollection("rental");

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

        System.out.println("i. Eine Sicht auf die Kunden mit allen relevanten Informationen wie im View „customer_list“ der vorhandenen Postgres-Datenbank");
        System.out.println("--> Query: Read.java, Z.404 - 488");
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

        getDbmongo().createView("customer_list","customer", pipeline);
        collection.aggregate(pipeline)
                .allowDiskUse(false)
                .forEach(processBlock);

        System.out.println("Show View");
        collection = getDbmongo().getCollection("customer_list");
        Document query = new Document();
        int limit = 5;
        collection.find(query).limit(limit).forEach(processBlock);
    }

    /* Getter and Setter */
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
