import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main {

  public static void main(String[] args) throws Exception {
    List<Integer> inputSize = Arrays.asList(100000, 10000000, 20000000, 30000000, 40000000, 50000000);
    List<Integer> concurrencies = Arrays.asList(1, 2, 4);
    List<Integer> batchesPerCommit = Arrays.asList(25, 50);
    List<Integer> rowsPerBatch = Arrays.asList(5000, 30000);
    List<Test.Config.IndexCreation> indexCreations = Arrays.asList(Test.Config.IndexCreation.LATE, Test.Config.IndexCreation.EARLY);
    List<Test.Config.PrimaryKey> primaryKeys = Arrays.asList(Test.Config.PrimaryKey.AUTO_INC, Test.Config.PrimaryKey.SUPPLIED, Test.Config.PrimaryKey.NONE);

    inputSize.forEach(sz ->
      concurrencies.forEach(conc ->
        batchesPerCommit.forEach(b ->
          rowsPerBatch.forEach(r ->
            indexCreations.forEach(index ->
              primaryKeys.forEach(prim ->
                new Test(Test.Config.builder()
                  .concurrency(conc)
                  .batchesPerCommit(b)
                  .inputSize(sz)
                  .indexCreation(index)
                  .primaryKey(prim)
                  .random(new Random(100))
                  .rowsPerBatch(r)
                  .build()).run()))))));
  }
}
