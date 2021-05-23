import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RequiredArgsConstructor
public class Test {
  @Data
  @Builder
  public static class Config {
    private int inputSize;
    private int concurrency;
    private int batchesPerCommit;
    private int rowsPerBatch;
    private IndexCreation indexCreation;
    private PrimaryKey primaryKey;
    private Random random;

    public enum IndexCreation {
      EARLY, LATE
    }

    public enum PrimaryKey {
      AUTO_INC, SUPPLIED, NONE
    }
  }

  private final Config config;

  public void run() {
    System.out.println(config + " START =================");

    Util.withRateLogging(config + " run", () ->
      Util.withPool("jdbc:mysql://root:my-secret-pw@localhost:3306/db", pool -> {
        setupTable(pool);
        if (config.getIndexCreation() == Config.IndexCreation.EARLY) {
          setupIndex(pool);
        }
        if (config.getPrimaryKey() == Config.PrimaryKey.SUPPLIED) {
          setupSuppliedPrimaryKey(pool);
        } else {
          setupAutoIncPrimaryKey(pool);
        }

        long insertedData = insertData(pool);

        if (config.getIndexCreation() == Config.IndexCreation.LATE) {
          setupIndex(pool);
        }

        return insertedData;
      })
    );
  }

  private void setupAutoIncPrimaryKey(DataSource pool) {
    Util.withRateLogging(config + " setup auto_increment primary key", () ->
      Util.withTransaction(pool, conn -> {
        try {
          return conn.createStatement().executeUpdate("alter table aliens add column id bigint not null auto_increment primary key");
        } catch (SQLException throwables) {
          throwables.printStackTrace();
          return 0;
        }
      })
    );
  }

  private void setupSuppliedPrimaryKey(DataSource pool) {
    Util.withRateLogging(config + " setup supplied primary key", () ->
      Util.withTransaction(pool, conn -> {
        try {
          return conn.createStatement().executeUpdate("alter table aliens add column id bigint not null primary key");
        } catch (SQLException throwables) {
          throwables.printStackTrace();
          return 0;
        }
      })
    );
  }

  private void setupIndex(DataSource pool) {
    Util.withRateLogging(config + " setup index", () ->
      Util.withTransaction(pool, conn -> {
        try {
          return conn.createStatement().executeUpdate("alter table aliens add index(system, planet, species)");
        } catch (SQLException throwables) {
          throwables.printStackTrace();
          return 0;
        }
      })
    );
  }

  private void setupTable(DataSource pool) {
    Util.withRateLogging(config + " setup table", () ->
      Util.withTransaction(pool, conn -> {
        try {
          int a = conn.createStatement().executeUpdate("drop table if exists aliens");
          int b = conn.createStatement().executeUpdate("create table aliens (\n" +
            "\tsystem varchar(100) not null, \n" +
            "\tplanet varchar(100) not null,\n" +
            "\tspecies varchar(100) not null,\n" +
            "\tage int unsigned not null,\n" +
            "\tweight int unsigned not null,\n" +
            "\theight int unsigned not null\n" +
            ")");
          return a + b;
        } catch (SQLException throwables) {
          throwables.printStackTrace();
          return 0;
        }
      })
    );
  }

  private long insertData(DataSource pool) {
    return Util.withRateLogging(config + " insert data", () -> {
      AtomicLong insertedCount = new AtomicLong(0);
      BlockingQueue<List<List<Alien>>> batches = new LinkedBlockingQueue<>(config.concurrency * 2);
      List<Thread> threads = IntStream.range(0, config.concurrency)
        .mapToObj(ignored -> new Thread(() -> {
          while (true) {
            List<List<Alien>> batch = Collections.emptyList();
            try {
              batch = batches.take();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }

            if (batch.isEmpty()) {
              break;
            }

            long inserted = insertData(pool, batch);
            insertedCount.addAndGet(inserted);
          }
        }))
        .collect(Collectors.toList());
      threads.forEach(Thread::start);

      Flux.fromStream(Alien.generateAliens(config.random, config.inputSize))
        .buffer(config.rowsPerBatch)
        .buffer(config.batchesPerCommit)
        .subscribe(e -> {
          try {
            batches.put(e);
          } catch (InterruptedException interruptedException) {
            interruptedException.printStackTrace();
          }
        });

      threads.forEach(ignored -> {
        try {
          batches.put(Collections.emptyList());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      });

      for (Thread thread : threads) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      return insertedCount.get();
    });
  }

  private long insertData(DataSource pool, List<List<Alien>> batches) {
    long size = batches.stream().mapToLong(Collection::size).sum();
    return Util.withRateLogging(config + " insert data " + size, () ->
      Util.withTransaction(pool, conn -> {
        List<String> cols = config.primaryKey == Config.PrimaryKey.SUPPLIED
          ? Arrays.asList("system", "planet", "species", "age", "weight", "height", "id")
          : Arrays.asList("system", "planet", "species", "age", "weight", "height");
        int numCol = cols.size();
        String fields = "(" + String.join(",", cols) + ")";
        String placeholder = "(" + cols.stream().map(ignored -> "?").collect(Collectors.joining(",")) + ")";

        return batches.stream()
          .mapToLong(batch -> {
            String placeholders = batch.stream().map(ignored -> placeholder).collect(Collectors.joining(","));
            long inserted = 0;
            try (PreparedStatement statement = conn.prepareStatement("insert into aliens " + fields + " values " + placeholders)) {
              assignValuesToPlaceholders(numCol, batch, statement);
              inserted += statement.executeUpdate();
            } catch (SQLException throwables) {
              throwables.printStackTrace();
            }
            return inserted;
          })
          .sum();
      })
    );
  }

  private void assignValuesToPlaceholders(int numCol, List<Alien> batch, PreparedStatement statement) {
    for (int i = 0; i < batch.size(); i++) {
      Alien alien = batch.get(i);
      int offset = i * numCol;
      try {
        statement.setString(1 + offset, alien.getSystem());
        statement.setString(2 + offset, alien.getPlanet());
        statement.setString(3 + offset, alien.getSpecies());
        statement.setInt(4 + offset, alien.getAge());
        statement.setInt(5 + offset, alien.getWeight());
        statement.setInt(6 + offset, alien.getHeight());
        if (config.primaryKey == Config.PrimaryKey.SUPPLIED) {
          statement.setLong(7 + offset, alien.getId());
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
