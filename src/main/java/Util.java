import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.function.Function;
import java.util.function.Supplier;

public class Util {
  public static <T> T withPool(String jdbcUrl, Function<DataSource, T> action) {
    HikariConfig configuration = new HikariConfig();
    configuration.setJdbcUrl(jdbcUrl);
    try (HikariDataSource ds = new HikariDataSource(configuration)) {
      return action.apply(ds);
    }
  }

  @SneakyThrows
  public static <T> T withTransaction(DataSource pool, Function<Connection, T> action) {
    Connection connection = null;
    try {
      connection = pool.getConnection();
      connection.setAutoCommit(false);
      T result = action.apply(connection);
      connection.commit();
      return result;
    } finally {
      connection.close();
    }
  }

  public static <T extends Number> T withRateLogging(String msg, Supplier<T> supplier) {
    long start = System.currentTimeMillis();
    T result = supplier.get();
    long end = System.currentTimeMillis();
    long delta = end - start;
    double rate = result.doubleValue() / delta * 1000;
    System.out.println(end + " " + msg + " took " + delta + " ms. Rate " + rate + "/s");
    return result;
  }
}
