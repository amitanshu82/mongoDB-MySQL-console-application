import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;

public class CRUDHelper {

    /**
     * Display ALl products
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        MongoCursor<Document> cursor = collection.find().cursor();
        while(cursor.hasNext()){
           // Call printSingleCommonAttributes to display the attributes on the Screen
            PrintHelper.printSingleCommonAttributes(cursor.next());
       }

    }

    /**
     * Display top 5 Mobiles
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        Bson filter = eq("Category","Mobiles");
        MongoCursor<Document> cursor = collection.find(filter).limit(5).cursor();
        while(cursor.hasNext()){
            // Call printAllAttributes to display the attributes on the Screen
            PrintHelper.printAllAttributes(cursor.next());
        }
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        MongoCursor<Document> cursor = collection.find().sort(new Document("Category",-1)).projection(fields(excludeId())).cursor();
        while(cursor.hasNext()) {
            // Call printAllAttributes to display the attributes on the Screen
            PrintHelper.printAllAttributes(cursor.next());
        }
    }


    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(
                        Aggregates.group("$Category", Accumulators.sum("Count", 1)))).cursor();
        while(cursor.hasNext()){
            // Call printProductCountInCategory to display the attributes on the Screen
            PrintHelper.printProductCountInCategory(cursor.next());
        }
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        ArrayList<Document> filters = new ArrayList<Document>();
        filters.add(new Document("Category","Headphones"));
        filters.add(new Document("ConnectorType","Wired"));
        MongoCursor<Document> cursor = collection.find(new Document("$and",filters)).cursor();
        while(cursor.hasNext()) {
            // Call printAllAttributes to display the attributes on the Screen
            PrintHelper.printAllAttributes(cursor.next());
        }
    }

    /**
     * Import data from MySql to MongoDB
     * @param sqlConnection
     * @param collection
     */
    public static void importDataToMongoDB(Connection sqlConnection, MongoCollection<Document> collection) {
        List<Document> documentList = new ArrayList<Document>();
        Statement statement = null;
        ResultSet rs = null;
        String queryForMobiles  = "Select * from pgcdata.mobiles";
        String queryForHeadphones = "Select * from pgcdata.headphones";
        String queryForCamera = "Select * from pgcdata.cameras";
        try {
            statement = sqlConnection.createStatement();
            rs = statement.executeQuery(queryForMobiles);
            if(rs!=null){
                populateDataForMobiles(rs,documentList);
            }
            rs = statement.executeQuery(queryForHeadphones);
            if(rs!=null){
                populateDataForHeadphone(rs,documentList);
            }
            rs = statement.executeQuery(queryForCamera);
            if(rs!=null){
                populateDataForCamera(rs,documentList);
            }
            // Write data to collection products
            collection.insertMany(documentList);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                rs.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *  Populate data in Mongo collection products for Mobiles
     * @param rs
     * @param documentList
     */
    private static void populateDataForMobiles(ResultSet rs, List<Document> documentList) {
        try {
            while (rs.next()) {
                Document document = new Document().append("Category","Mobiles")
                        .append("ProductId",rs.getString("ProductId"))
                        .append("Title",rs.getString("Title"))
                        .append("Manufacturer",rs.getString("Manufacturer"))
                        .append("NetworkTechnology",rs.getString("NetworkTechnology"))
                        .append("Dimensions",rs.getString("Dimensions"))
                        .append("Weight",rs.getString("Weight"))
                        .append("Display",rs.getString("Display"))
                        .append("Bluetooth",rs.getString("Bluetooth"))
                        .append("Sensors",rs.getString("Sensors"))
                        .append("OS",rs.getString("OS"))
                        .append("Chipset",rs.getString("Chipset"))
                        .append("CPU",rs.getString("CPU"))
                        .append("GPU",rs.getString("GPU"))
                        .append("Memory",rs.getString("Memory"))
                        .append("Camera",rs.getString("Camera"))
                        .append("Battery",rs.getString("Battery"));
                documentList.add(document);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Populate data for Headphones
     * @param rs
     * @param documentList
     */
    private static void populateDataForHeadphone(ResultSet rs,List<Document> documentList) {
        try {
            while (rs.next()) {
                Document document = new Document().append("Category","Headphones")
                        .append("ProductId",rs.getString("ProductId"))
                        .append("Title",rs.getString("Title"))
                        .append("Manufacturer",rs.getString("Manufacturer"))
                        .append("HeadPhoneType",rs.getString("HeadPhoneType"))
                        .append("Battery",rs.getString("Battery"))
                        .append("Warranty",rs.getString("Warranty"))
                        .append("ConnectorType",rs.getString("ConnectorType"))
                        .append("WithMicrophone",rs.getString("WithMicrophone"))
                        .append("ItemWeight",rs.getString("ItemWeight"))
                        .append("Color",rs.getString("Color"))
                        .append("AdditionalFeatures",rs.getString("AdditionalFeatures"));
                documentList.add(document);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Populate data for Cameras
     * @param rs
     * @param documentList
     */
    private static void populateDataForCamera(ResultSet rs,List<Document> documentList) {
        try {
            while (rs.next()) {
                Document document = new Document().append("Category","Cameras")
                        .append("ProductId",rs.getString("ProductId"))
                        .append("Title",rs.getString("Title"))
                        .append("Manufacturer",rs.getString("Manufacturer"))
                        .append("EffectivePixels",rs.getString("EffectivePixels"))
                        .append("Zoom",rs.getString("Zoom"))
                        .append("Dimension",rs.getString("Dimension"))
                        .append("Weight",rs.getString("Weight"))
                        .append("VideoResolution",rs.getString("VideoResolution"))
                        .append("ShutterSpeed",rs.getString("ShutterSpeed"))
                        .append("Battery",rs.getString("Battery"));
                documentList.add(document);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}