import java.awt.*;
import java.text.SimpleDateFormat;

public class Main {
    public static void main(String[] args) {
        System.out.println("Log from: " +  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
        System.out.println("TEST2");
        System.out.println("CRUD operations started\n");

        System.out.println("Create operations started");
        new Create();
        System.out.println("Create operations completed\n");

        System.out.println("Read operations started");
        new Read();
        System.out.println("Read operations completed\n");

        System.out.println("Update operations started");
        new Update();
        System.out.println("Update operations completed\n");

        System.out.println("Delete operations started");
        new Delete();
        System.out.println("Delete operations completed\n");

        System.out.println("CRUD operations completed");
    }
}
