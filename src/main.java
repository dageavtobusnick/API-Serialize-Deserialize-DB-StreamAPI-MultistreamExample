import com.mysql.cj.conf.ConnectionUrlParser;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class main {
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, JSONException {
        System.out.println("Для работы с бд необходимо указать необходимые атрибуты в файле db.json");
        Path file = Paths.get("db.json");
        Scanner scanner = new Scanner(Files.newInputStream(file));
        String line = scanner.nextLine();
        JSONObject object = new JSONObject(line);
        Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
        Connection conn = DriverManager.getConnection("");
        try {
            conn = DriverManager.getConnection(String.format("jdbc:mysql://%s:%d/%s?serverTimezone=Europe/Moscow&useSSL=false", object.getString("ip"), object.getInt("port"), object.getString("db_name")), object.getString("login"), object.getString("password"));
            createTable(conn);
        } catch (SQLException ignored) {
        }
        while (true) {
            System.out.println("Введите: \n\r1, если хотите получить данные с api и вывести их на экран\n\r2, если хотите получить данные с api и сериализовать их\n\r3, если хотите десериализовать файл и вывести данные на экран\n\r4, если хотите дополнить базу данных,\n\r5, если хотите вывести статистику,\n\r6, если хотите получить статистику асинхронно.\n\r7, если хотите выйти");
            boolean exitMarker = false;
            switch (new Scanner(System.in).next()) {
                case "1" -> getDataFromAPI();
                case "2" -> serializeCollection();
                case "3" -> deserializeCollection();
                case "4" -> getDataAndFillTable(conn);
                case "5" -> getStatistic(conn);
                case "7" -> exitMarker = true;
                case "6" -> getStatisticsAsync(conn);


            }
            if (exitMarker) {
                conn.close();
                break;
            }
        }
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    private static void deserializeCollection() throws IOException, ClassNotFoundException, JSONException {
        System.out.println("Выберите тип сериализации\n\r 1-Json\n\r 2-бинарная");
        String type = new Scanner(System.in).next();
        System.out.println("Введите имя файла, в который хотите сериализовать коллекцию");
        String fileName = new Scanner(System.in).next();
        switch (type) {
            case "1" -> jsonDeserialize(fileName);
            case "2" -> binDeserialize(fileName);
        }
    }

    private static void binDeserialize(String fileName) throws IOException, ClassNotFoundException {
        String fileNameWithFormat = String.format("%s.bin", fileName);
        try (ObjectInputStream ois = new ObjectInputStream(Files.newInputStream(Path.of(fileNameWithFormat)))) {
            GroupData data = (GroupData) ois.readObject();
            data.print();
        }

    }

    private static void jsonDeserialize(String fileName) throws IOException, JSONException {
        String fileNameWithFormat = String.format("%s.json", fileName);
        Path file = Paths.get(fileNameWithFormat);
        Scanner scanner = new Scanner(Files.newInputStream(file));
        String line = scanner.nextLine();
        JSONObject object = new JSONObject(line);
        scanner.close();
        GroupData groupData = new GroupData(new TreeMap<>(), (String) object.keys().next());
        JSONObject workCollection = object.getJSONObject((String) object.keys().next());
        var keys = workCollection.keys();
        while (keys.hasNext()) {
            String key = (String) keys.next();
            groupData.data.put(key, workCollection.getInt(key));
        }
        groupData.print();
    }

    private static void serializeCollection() throws IOException, JSONException {
        System.out.println("Введите имя или id группы,данные для которой нужно получить с API и сериализовать");
        String groupName = new Scanner(System.in).next();
        JSONArray items = getJsonArray(groupName);
        TreeMap<String, Integer> cities = new TreeMap<>();
        fillCollection(items, cities);
        GroupData data = new GroupData(cities, groupName);
        System.out.println("Выберите тип сериализации\n\r 1-Json\n\r 2-бинарная");
        String type = new Scanner(System.in).next();
        System.out.println("Введите имя файла, в который хотите сериализовать коллекцию");
        String fileName = new Scanner(System.in).next();
        switch (type) {
            case "1" -> jsonSerialize(data, fileName);
            case "2" -> binSerialise(data, fileName);
        }
    }

    private static void binSerialise(GroupData cities, String fileName) throws IOException {
        String fileNameWithFormat = String.format("%s.bin", fileName);
        Path file = createFile(fileNameWithFormat);
        try (ObjectOutputStream oos = new ObjectOutputStream(Files.newOutputStream(file))) {
            oos.writeObject(cities);
        }
    }

    private static void jsonSerialize(GroupData cities, String fileName) throws IOException, JSONException {
        JSONObject data = new JSONObject();
        data.put(cities.groupId, cities.data);
        String fileNameWithFormat = String.format("%s.json", fileName);
        Path file = createFile(fileNameWithFormat);
        FileWriter fw = new FileWriter(file.toFile());
        fw.write(data.toString());
        fw.close();
    }

    private static Path createFile(String fileNameWithFormat) throws IOException {
        Path file = Path.of(fileNameWithFormat);
        if (Files.notExists(file)) {
            Files.createFile(file);
        }
        return file;
    }

    private static void getDataFromAPI() throws IOException, JSONException {
        System.out.println("Введите имя или id группы,данные для которой нужно получить с API");
        String groupName = new Scanner(System.in).next();
        JSONArray items = getJsonArray(groupName);
        TreeMap<String, Integer> cities = new TreeMap<>();
        fillCollection(items, cities);
        cities.keySet().forEach(x -> System.out.printf("В группе %s %d людей из города %s\n\r", groupName, cities.get(x), x));
    }

    private static void getStatisticsAsync(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("SELECT groupId  FROM `cities` GROUP BY groupId");
        ResultSet resultSet = stmt.executeQuery();
        int processorsCount = Runtime.getRuntime().availableProcessors();
        ArrayDeque<String> groups = new ArrayDeque<>();
        List<StatisticThread> threads = new LinkedList<>();
        while (resultSet.next())
            groups.add(resultSet.getString("groupId"));
        while (true) {
            if (threads.size() < processorsCount && groups.size() != 0) {
                StatisticThread statisticThread = new StatisticThread(conn, groups.pop());
                threads.add(statisticThread);
                statisticThread.start();
                try {
                    statisticThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (threads.size() == 0)
                break;
            threads = threads.stream().filter(Thread::isAlive).collect(Collectors.toList());
        }
    }

    private static void getStatistic(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM `cities`");
        ResultSet data = stmt.executeQuery();
        HashMap<String, List<ConnectionUrlParser.Pair<String, Integer>>> result = new HashMap<>();
        while (data.next()) {
            String groupId = data.getString("groupId");
            int count = data.getInt("count");
            String name = data.getString("name");
            if (!result.containsKey(groupId))
                result.put(groupId, new LinkedList<>());
            result.get(groupId).add(new ConnectionUrlParser.Pair<>(name, count));
        }
        for (String groupId : result.keySet()
        ) {
            System.out.printf("Статистика для группы: %s%n", groupId);
            AtomicInteger membersCount = new AtomicInteger();
            result.get(groupId).forEach(x -> membersCount.addAndGet(x.right));
            result.get(groupId).forEach(x -> System.out.printf("%s:%d(%f%s)%n", x.left, x.right, (x.right / (double) membersCount.get()) * 100, "%"));
            ConnectionUrlParser.Pair<String, Integer> max = result.get(groupId).stream()
                    .max((x, y) -> (x.right > y.right) ? 1 : (x.right < y.right) ? -1 : 0).get();
            System.out.printf("Максимум: %s %d(%f%s)%n", max.left, max.right, max.right * 100 / (double) membersCount.get(), "%");
        }

    }

    private static void getDataAndFillTable(Connection conn) throws IOException, JSONException, SQLException {
        System.out.println("Введите имя или id группы");
        String groupName = new Scanner(System.in).next();
        JSONArray items = getJsonArray(groupName);
        TreeMap<String, Integer> cities = new TreeMap<>();
        fillCollection(items, cities);
        fillTable(cities, conn, groupName);

    }

    private static JSONArray getJsonArray(String groupName) throws IOException, JSONException {
        URL url = new URL(String.format("https://api.vk.com/method/groups.getMembers?group_id=%s&fields=city&access_token=09f70e8a8e7f57ebd6fc113c2067b3f3e79003fd2b5a83fa9f12b07f3c27ff12ff336dbda30edb0058054&v=5.124", groupName));
        URLConnection yc = url.openConnection();
        BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
        String inputLine = readAll(in);
        in.close();
        JSONObject json = new JSONObject(inputLine);
        return json.getJSONObject("response").getJSONArray("items");
    }


    private static void fillTable(TreeMap<String, Integer> cities, Connection conn, String groupName) throws SQLException {
        for (String name : cities.keySet()
        ) {
            PreparedStatement stmt2 = conn.prepareStatement("INSERT INTO `cities`(`name`, `groupId`) SELECT ?,? WHERE NOT EXISTS (SELECT name,groupId FROM `cities` WHERE name=? AND groupId=?)");
            stmt2.setString(1, name);
            stmt2.setString(2, groupName);
            stmt2.setString(3, name);
            stmt2.setString(4, groupName);
            stmt2.execute();
            PreparedStatement stmt3 = conn.prepareStatement("UPDATE `cities` SET `count`=? WHERE name=? AND groupId=?");
            stmt3.setInt(1, cities.get(name));
            stmt3.setString(2, name);
            stmt3.setString(3, groupName);
            stmt3.execute();
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("CREATE TABLE IF NOT EXISTS `example`.`cities` ( `Id` INT(100) UNSIGNED NOT NULL AUTO_INCREMENT, PRIMARY KEY (`Id`), `name` VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL , `count` INT(100) NOT NULL DEFAULT '0' ,`groupId` VARCHAR(255) NOT NULL ) ENGINE = InnoDB;");
        stmt.execute();
    }

    private static void fillCollection(JSONArray items, TreeMap<String, Integer> cities) {
        for (int i = 0; i < items.length(); i++) {
            try {
                String city = items.getJSONObject(i).getJSONObject("city").getString("title");
                if (!cities.containsKey(city))
                    cities.put(city, 1);
                else cities.put(city, cities.get(city) + 1);
            } catch (JSONException ignored) {
            }
        }
    }

    private static class GroupData implements Serializable {
        private final TreeMap<String, Integer> data;
        private final String groupId;

        private GroupData(TreeMap<String, Integer> data, String groupId) {
            this.data = data;
            this.groupId = groupId;
        }

        public void print() {
            data.keySet().forEach(x -> System.out.printf("В группе %s %d людей из города %s\n\r", groupId, data.get(x), x));
        }
    }

    static class StatisticThread extends Thread {
        public final Connection connection;
        public final String groupId;

        public StatisticThread(Connection connection, String groupId) {
            this.connection = connection;
            this.groupId = groupId;
        }

        @Override
        public void run() {
            PreparedStatement stmt;
            try {
                stmt = connection.prepareStatement("SELECT * FROM `cities` WHERE groupId=?");
                stmt.setString(1, groupId);
                ResultSet data = stmt.executeQuery();
                List<ConnectionUrlParser.Pair<String, Integer>> result = new LinkedList<>();
                while (data.next()) {
                    int count = data.getInt("count");
                    String name = data.getString("name");
                    result.add(new ConnectionUrlParser.Pair<>(name, count));
                }
                System.out.printf("Статистика для группы: %s%n", groupId);
                AtomicInteger membersCount = new AtomicInteger();
                result.forEach(x -> membersCount.addAndGet(x.right));
                result.forEach(x -> System.out.printf("%s:%d(%f%s)%n", x.left, x.right, (x.right / (double) membersCount.get()) * 100, "%"));
                ConnectionUrlParser.Pair<String, Integer> max = result.stream()
                        .max((x, y) -> (x.right > y.right) ? 1 : (x.right < y.right) ? -1 : 0).get();
                System.out.printf("Максимум: %s %d(%f%s)%n", max.left, max.right, max.right * 100 / (double) membersCount.get(), "%");
            } catch (SQLException throwable) {
                throwable.printStackTrace();
            }
        }

    }
}

