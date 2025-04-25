import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.Scanner;

public class Main {
    private static final String DB_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            initializeMusicTable(conn);
            initializeBooksAndVisitors(conn);
            executeTasks(conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initializeMusicTable(Connection conn) throws Exception {
        String sql = readResourceFile("music-create.sql");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS study");
            stmt.execute(sql);
            System.out.println("Таблица music успешно создана и заполнена");
        }
    }

    private static void initializeBooksAndVisitors(Connection conn) throws Exception {
        String json = readResourceFile("books.json");
        Gson gson = new Gson();
        List<Visitor> visitors = gson.fromJson(json, new TypeToken<List<Visitor>>(){}.getType());

        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS visitors (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "name VARCHAR(50) NOT NULL, " +
                    "surname VARCHAR(50) NOT NULL, " +
                    "phone VARCHAR(20), " +
                    "subscribed BOOLEAN)");

            stmt.execute("CREATE TABLE IF NOT EXISTS books (" +
                    "id INT AUTO_INCREMENT PRIMARY KEY, " +
                    "title VARCHAR(100) NOT NULL, " +
                    "author VARCHAR(100) NOT NULL, " +
                    "publishing_year INT, " +
                    "isbn VARCHAR(20), " +
                    "publisher VARCHAR(100), " +
                    "visitor_id INT, " +
                    "FOREIGN KEY (visitor_id) REFERENCES visitors(id))");
        }

        for (Visitor visitor : visitors) {
            int visitorId;
            String insertVisitor = "INSERT INTO visitors (name, surname, phone, subscribed) VALUES (?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertVisitor, Statement.RETURN_GENERATED_KEYS)) {
                pstmt.setString(1, visitor.name);
                pstmt.setString(2, visitor.surname);
                pstmt.setString(3, visitor.phone);
                pstmt.setBoolean(4, visitor.subscribed);
                pstmt.executeUpdate();

                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        visitorId = rs.getInt(1);
                    } else {
                        throw new SQLException("Не удалось получить ID посетителя");
                    }
                }
            }

            String insertBook = "INSERT INTO books (title, author, publishing_year, isbn, publisher, visitor_id) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertBook)) {
                for (Book book : visitor.favoriteBooks) {
                    pstmt.setString(1, book.name);
                    pstmt.setString(2, book.author);
                    pstmt.setInt(3, book.publishingYear);
                    pstmt.setString(4, book.isbn);
                    pstmt.setString(5, book.publisher);
                    pstmt.setInt(6, visitorId);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
        }
        System.out.println("Таблицы visitors и books успешно созданы и заполнены");
    }

    private static void executeTasks(Connection conn) throws SQLException {
        System.out.println("\n=== Результаты выполнения заданий ===");

        // 1. Список музыкальных композиций
        printAllMusic(conn);

        // 2. Композиции без букв 'm' и 't'
        printMusicWithoutMt(conn);

        // 3. Добавление композиций и их вывод
        addFavoriteMusic(conn, "Chasm");
        addFavoriteMusic(conn, "Versus");
        printAllMusic(conn);

        // 4. Отсортированный список книг по году издания
        printBooksSortedByYear(conn);

        // 5. Книги младше 2000 года
        printBooksAfter2000(conn);

        // 6. Добавление информации о пользователях
        int Id_1 = createUser(conn, "Владимир", "Миллер", true);
        addBook(conn, "Страна самоцветов", "Харуко Итикава", 2012, Id_1);
        addBook(conn, "Скарамуш", "Рафаэль Сабатини", 1921, Id_1);
        addBook(conn, "Старикам тут не место", "Кормака Маккарти", 2005, Id_1);

        int Id_2 = createUser(conn, "Алина", "Новожилова", true);
        addBook(conn, "На Западном фронте без перемен", "Эрих Мария Ремарк", 1929, Id_2);
        addBook(conn, "Над кукушкиным гнездом", "Кен Кизи", 1962, Id_2);

        int Id_3 = createUser(conn, "Ярослав", "Решетников", true);
        addBook(conn, "Золотой храм", "Юкио Мисима", 1956, Id_3);
        // 7. Вывод информации о пользователях
        System.out.printf("\nЛюбимые книги пользователей:\n");
        printPersonalBooks(conn, Id_1);
        printPersonalBooks(conn, Id_2);
        printPersonalBooks(conn, Id_3);

        // 8. Удаление таблиц
        dropTables(conn);
    }

    private static void printAllMusic(Connection conn) throws SQLException {
        System.out.println("\n1. Список музыкальных композиций:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM study.music")) {
            while (rs.next()) {
                System.out.println(rs.getInt("id") + ": " + rs.getString("name"));
            }
        }
    }

    private static void printMusicWithoutMt(Connection conn) throws SQLException {
        System.out.println("\n2. Композиции без букв 'm' и 't':");
        String sql = "SELECT * FROM study.music " +
                "WHERE LOWER(name) NOT LIKE '%m%' " +
                "AND LOWER(name) NOT LIKE '%t%'";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println(rs.getInt("id") + ": " + rs.getString("name"));
            }
        }
    }

    private static void addFavoriteMusic(Connection conn, String musicName) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO study.music (id, name) VALUES (?, ?)")) {
            int maxId = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM study.music")) {
                if (rs.next()) {
                    maxId = rs.getInt(1);
                }
            } catch (SQLException e) {
                System.err.println("Ошибка при получении максимального ID: " + e.getMessage());
                throw e;
            }

            pstmt.setInt(1, maxId + 1);
            pstmt.setString(2, musicName);

            int affectedRows = pstmt.executeUpdate();

            /*if (affectedRows > 0) {
                System.out.println("Композиция '" + musicName + "' добавлена!");
            } else {
                System.out.println("Не удалось добавить композицию.");
            }*/

        } catch (SQLException e) {
            System.err.println("Ошибка при добавлении композиции: " + e.getMessage());
            throw e;
        }
    }

    private static void printBooksSortedByYear(Connection conn) throws SQLException {
        System.out.println("\n4. Книги, отсортированные по году издания:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT title, publishing_year, author FROM books GROUP BY title, publishing_year, author ORDER BY publishing_year")) {
            while (rs.next()) {
                System.out.printf("%s (%d) by %s%n",
                        rs.getString("title"),
                        rs.getInt("publishing_year"),
                        rs.getString("author"));
            }
        }
    }


    private static void printBooksAfter2000(Connection conn) throws SQLException {
        System.out.println("\n5. Книги, изданные после 2000 года:");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT title, publishing_year, author FROM books WHERE publishing_year >= 2000 GROUP BY title, publishing_year, author ORDER BY publishing_year")) {
            while (rs.next()) {
                System.out.printf("%s (%d) by %s%n",
                        rs.getString("title"),
                        rs.getInt("publishing_year"),
                        rs.getString("author"));
            }
        }
    }

    private static int createUser(Connection conn, String name, String surname, boolean subscribed) throws SQLException {
        String insertVisitor = "INSERT INTO visitors (name, surname, subscribed) VALUES (?, ?, ?)";
        int myId = -1;

        try (PreparedStatement pstmt = conn.prepareStatement(insertVisitor, Statement.RETURN_GENERATED_KEYS)) {
            pstmt.setString(1, name);
            pstmt.setString(2, surname);
            pstmt.setBoolean(3, subscribed);
            pstmt.executeUpdate();

            try (ResultSet rs = pstmt.getGeneratedKeys()) {
                if (rs.next()) {
                    myId = rs.getInt(1);
                } else {
                    throw new SQLException("Не удалось получить ID добавленного пользователя.");
                }
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при создании пользователя: " + e.getMessage());
            throw e;
        }
        return myId;
    }

    private static void addBook(Connection conn, String title, String author, int publishingYear, int visitorId) throws SQLException {
        String insertBook = "INSERT INTO books (title, author, publishing_year, visitor_id) VALUES (?, ?, ?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertBook)) {
            pstmt.setString(1, title);
            pstmt.setString(2, author);
            pstmt.setInt(3, publishingYear);
            pstmt.setInt(4, visitorId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Ошибка при добавлении книги: " + e.getMessage());
            throw e;
        }
    }

    private static void printPersonalBooks(Connection conn, int visitorId) throws SQLException {
        String query = "SELECT v.name, v.surname, b.title " +
                "FROM visitors v JOIN books b ON v.id = b.visitor_id " +
                "WHERE v.id = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, visitorId);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    System.out.printf("\"%s %s\": ", rs.getString("name"), rs.getString("surname")); // Выводим имя один раз

                    StringBuilder bookList = new StringBuilder();
                    do {
                        bookList.append(rs.getString("title")).append(", ");
                    } while (rs.next());

                    bookList.delete(bookList.length() - 2, bookList.length());
                    System.out.println(bookList.toString());

                } else {
                    System.out.println("У этого пользователя нет любимых книг.");
                }
            }
        } catch (SQLException e) {
            System.err.println("Ошибка при выполнении запроса: " + e.getMessage());
            throw e;
        }
    }


    private static void dropTables(Connection conn) throws SQLException {
        System.out.println("\n8. Удаляем таблицы visitors и books...");
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS books");
            stmt.execute("DROP TABLE IF EXISTS visitors");
            System.out.println("Таблицы удалены!");
        }
    }

    private static String readResourceFile(String filename) throws Exception {
        InputStream is = Main.class.getClassLoader().getResourceAsStream(filename);
        if (is == null) {
            throw new FileNotFoundException("Файл " + filename + " не найден в ресурсах");
        }
        try (Scanner scanner = new Scanner(is, StandardCharsets.UTF_8.name()).useDelimiter("\\A")) {
            return scanner.hasNext() ? scanner.next() : "";
        }
    }
    
    static class Visitor {
        String name;
        String surname;
        String phone;
        boolean subscribed;
        List<Book> favoriteBooks;
    }

    static class Book {
        String name;
        String author;
        int publishingYear;
        String isbn;
        String publisher;
    }
}