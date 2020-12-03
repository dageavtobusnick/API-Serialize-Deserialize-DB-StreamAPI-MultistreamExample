import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.TreeMap;


public class main {
    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, JSONException {
        URL url = new URL("https://api.vk.com/method/groups.getMembers?group_id=iritrtf_urfu&fields=city&access_token=09f70e8a8e7f57ebd6fc113c2067b3f3e79003fd2b5a83fa9f12b07f3c27ff12ff336dbda30edb0058054&v=5.124");
        URLConnection yc = url.openConnection();
        BufferedReader in = new BufferedReader(
                new InputStreamReader(
                        yc.getInputStream()));
        String inputLine = readAll(in);
        JSONObject json = new JSONObject(inputLine);
        JSONArray items = json.getJSONObject("response").getJSONArray("items");
        TreeMap<String,Integer> cities= new TreeMap<>();
        fillCollection(items, cities);
        in.close();
        Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/example?serverTimezone=Europe/Moscow&useSSL=false", "root", "root");
        createTable(conn);
        fillTable(cities, conn);
    }

    private static void fillTable(TreeMap<String, Integer> cities, Connection conn) throws SQLException {
        for (String name: cities.keySet()
             ) {
            PreparedStatement stmt2= conn.prepareStatement("INSERT INTO `cities`(`name`, `count`, `groupId`) VALUES (?,?,?)");
            stmt2.setString(1,name);
            stmt2.setInt(2, cities.get(name));
            stmt2.setString(3,"iritrtf_urfu");
            stmt2.execute();
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        PreparedStatement stmt= conn.prepareStatement("CREATE TABLE IF NOT EXISTS `example`.`cities` ( `Id` INT(100) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY (`Id`), `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL , `count` INT(100) NOT NULL ,`groupId` VARCHAR(255) NOT NULL ) ENGINE = InnoDB;");
        stmt.execute();
    }

    private static void fillCollection(JSONArray items, TreeMap<String, Integer> cities) {
        for (int i = 0; i< items.length(); i++) {
            try {
                String city = items.getJSONObject(i).getJSONObject("city").getString("title");
                if (!cities.containsKey(city))
                    cities.put(city, 1);
                else cities.put(city, cities.get(city) + 1);
            }catch(JSONException e){
                continue;
            }
        }
    }
}

