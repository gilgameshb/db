import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class Delete {
    private MongoClient client;
    private MongoDatabase dbmongo;

    public Delete() {
        // Connect to mongoDB
        setClient(MongoClients.create("mongodb://mongo:1234@mongo_db:27017"));
        //setClient(MongoClients.create("mongodb://mongo:1234@localhost:27017"));

        // Get database with the name example
        setDbmongo(client.getDatabase("dvdrental"));

        delete();
    }

    private void delete() {
        System.out.println("a. Löscht alle Filme, die weniger als 60 Minuten Spielzeit haben");
        System.out.println("--> Query: Delete.java, Z. 31-43");

        MongoCollection<Document> collectionFilms = getDbmongo().getCollection("film");

        long amountFilmsBeforeDelete = collectionFilms.countDocuments();

        List<String> deleteFilmIds = new ArrayList<>();

        for (Document nextDocument : collectionFilms.find()) {
            int length = Integer.parseInt(nextDocument.getString("length"));
            if (length < 60) {
                collectionFilms.deleteMany(eq("film_id", nextDocument.getString("film_id")));
                deleteFilmIds.add(nextDocument.getString("film_id"));
            }
        }

        System.out.println("    Anzahl der Filme vor dem Löschen: " + amountFilmsBeforeDelete);
        System.out.println("    Anzahl der Filme nach dem Löschen: " + collectionFilms.countDocuments());
        System.out.println("    " + deleteFilmIds.size() + " Filme wurden gelöscht");

        /*
        Die ausgeliehenden Filme sind in rental gelistet.
        Da also neben dem Löschen der Filme auch die Dokumente der entsprechenden Filme in rental gelöscht werden sollen,
        macht es Sinn, die entsprechenden Dokumente in inventory zu entfernen, da diese dann nicht mehr benötigt werden.
        Die Dokumente payment sollten aufgrund der Buchhaltung bestehen bleiben.
         */
        System.out.println("b. Löscht alle damit zusammenhängenden Entleihungen");
        System.out.println("--> Query: Delete.java, Z. 58-72, 78-83");

        MongoCollection<Document> collectionInventory = getDbmongo().getCollection("inventory");
        MongoCollection<Document> collectionRental = getDbmongo().getCollection("rental");

        long amountFilmsInInventoryBeforeDelete = collectionInventory.countDocuments();
        long amountFilmsInRentalBeforeDelete = collectionRental.countDocuments();

        List<String> deleteInventoryIds = new ArrayList<>();
        List<String> deleteRentalIds = new ArrayList<>();

        for (Document nextDocument : collectionInventory.find()) {
            if (deleteFilmIds.contains(nextDocument.getString("film_id"))) {
                collectionInventory.deleteMany(eq("film_id", nextDocument.getString("film_id")));
                deleteInventoryIds.add(nextDocument.getString("inventory_id"));
            }
        }

        System.out.println("    Anzahl der Filme im Inventar vor dem Löschen: " + amountFilmsInInventoryBeforeDelete);
        System.out.println("    Anzahl der Filme im Inventar nach dem Löschen: " + collectionInventory.countDocuments());
        System.out.println("    " + deleteInventoryIds.size() + " Inventory-Dokumente wurden gelöscht");

        for (Document nextDocument : collectionRental.find()) {
            if (deleteInventoryIds.contains(nextDocument.getString("inventory_id"))) {
                collectionRental.deleteMany(eq("inventory_id", nextDocument.getString("inventory_id")));
                deleteRentalIds.add(nextDocument.getString("rental_id"));
            }
        }

        System.out.println("    Anzahl der Ausleihungen vor dem Löschen: " + amountFilmsInRentalBeforeDelete);
        System.out.println("    Anzahl der Ausleihungen nach dem Löschen: " + collectionRental.countDocuments());
        System.out.println("    " + deleteRentalIds.size() + " Ausleihungen wurden gelöscht");
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
