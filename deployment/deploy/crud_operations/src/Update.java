import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import org.bson.Document;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;

public class Update {
    private MongoClient client;
    private MongoDatabase dbmongo;

    public Update() {
        // Connect to mongoDB and get database
        setClient(MongoClients.create("mongodb://mongo:1234@mongo_db:27017"));
        setDbmongo(client.getDatabase("dvdrental"));

        updateA();
        updateB();
    }

    private void updateA() {
        System.out.println("a. Vergebt allen Mitarbeitern ein neues, sicheres Passwort");
        System.out.println("--> Query: Update.java, Z. 38-39");

        MongoCollection<Document> collection = getDbmongo().getCollection("staff");

        // Ändert für jeden Mitarbeiter pasword und last_update
        for (Document nextDocument : collection.find()) {
            collection.updateOne(eq("password", nextDocument.get("password")), new Document("$set", new Document("password", randomizePassword())));
            collection.updateOne(eq("last_update", nextDocument.get("last_update")), new Document("$set", new Document("last_update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()))));
        }

        for (Document doc : collection.find()) {
            System.out.println("    " + doc.toJson());
        }
    }

    private void updateB() {
        System.out.println("b. Erzeugt einen neuen Standort (mit einer fiktiven Adresse) und verlegt das Inventar der beiden bisherigen Standorte dorthin");
        System.out.println("--> Query: Update.java, Z. 51-70, 77-89");

        MongoCollection<Document> collection = getDbmongo().getCollection("store");

        // Erstellen des neuen Stores
        BasicDBObject searchQuery = new BasicDBObject("store_id", "3");
        BasicDBObject updateFields = new BasicDBObject();
        updateFields
                .append("store_id", "3")
                .append("manager_staff_id", "3");

        BasicDBObject setQuery = new BasicDBObject();
        setQuery.append("$set", updateFields);
        collection.findOneAndUpdate(searchQuery, setQuery, new FindOneAndUpdateOptions().upsert(true));

        // Necessary for the order of fields to equal those in MongoDB
        updateFields
                .append("address_id", "3")
                .append("last_update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
                );
        setQuery.append("$set", updateFields);
        collection.findOneAndUpdate(searchQuery, setQuery, new FindOneAndUpdateOptions().upsert(true));

        for (Document doc : collection.find()) {
            System.out.println("    " + doc.toJson());
        }

        // Ändere die store_id für alle Dokumente in inventory auf "3"
        MongoCollection<Document> collectionInv = getDbmongo().getCollection("inventory");

        BasicDBObject updateFieldsStore = new BasicDBObject();
        BasicDBObject searchQueryStore = new BasicDBObject();
        BasicDBObject setQueryStore = new BasicDBObject();

        // Ändert für jedes Dokument die store_id und last_update
        updateFieldsStore
                .append("store_id", "3")
                .append("last_update", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date())
                );
        setQueryStore.append("$set", updateFieldsStore);
        collectionInv.updateMany(searchQueryStore, setQueryStore);

        System.out.println("    Die ersten 10 Einträge aus inventory:");
        int i = 0;
        for (Document doc : collectionInv.find()) {
            if (i < 10) {
                System.out.println("    " + doc.toJson());
            }
            i++;
        }
    }

    /**
     * Generates a random password
     * <p>
     * Sourcecode: https://www.tutorialspoint.com/Generating-password-in-Java
     * Some few adjustments were made to the code.
     *
     * @return Password as hashed String
     */
    private String randomizePassword() {
        String capitalCaseLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String lowerCaseLetters = "abcdefghijklmnopqrstuvwxyz";
        String specialCharacters = ".*[!@#&()–[{}]:;',?/*~$^+=<>]";
        String numbers = "1234567890";
        String combinedChars = capitalCaseLetters + lowerCaseLetters + specialCharacters + numbers;

        Random random = new Random();
        int length = random.nextInt(8, 20);
        char[] password = new char[length];

        password[0] = lowerCaseLetters.charAt(random.nextInt(lowerCaseLetters.length()));
        password[1] = capitalCaseLetters.charAt(random.nextInt(capitalCaseLetters.length()));
        password[2] = specialCharacters.charAt(random.nextInt(specialCharacters.length()));
        password[3] = numbers.charAt(random.nextInt(numbers.length()));

        StringBuilder passwordString = new StringBuilder();
        for (int i = 4; i < length; i++) {
            password[i] = combinedChars.charAt(random.nextInt(combinedChars.length()));
            passwordString.append(password[i]);
        }

        return encryptPassword(passwordString.toString());
    }

    /**
     * Encrypts a password with salt and PBKDF2
     * <p>
     * Sourcecode: https://www.baeldung.com/java-password-hashing
     * Some few adjustments were made to the code.
     *
     * @param password Password that should be hashed
     * @return Hashed password
     */
    private String encryptPassword(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] messageDigest = md.digest(password.getBytes());
            BigInteger no = new BigInteger(1, messageDigest);

            StringBuilder hashtext = new StringBuilder(no.toString(16));
            while (hashtext.length() < 32) {
                hashtext.insert(0, "0");
            }
            return hashtext.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
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
