import lombok.Builder;
import lombok.Data;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Data
@Builder
class Alien {
  private long id;
  private String system;
  private String planet;
  private String species;
  private int age;
  private int weight;
  private int height;

  public static Stream<Alien> generateAliens(Random random, int count) {
    return IntStream.range(0, count).mapToObj(idx -> buildAlien(idx, random));
  }

  private static Alien buildAlien(int idx, Random random) {
    return Alien.builder()
      .id(idx)
      .system("system" + randomRange(random, 50))
      .planet("planet" + randomRange(random, 500))
      .species("species" + randomRange(random, 10000))
      .age(randomRange(random, 10000))
      .weight(randomRange(random, 200000))
      .height(randomRange(random, 200000))
      .build();
  }

  private static int randomRange(Random random, int max) {
    return Math.abs(random.nextInt()) % max;
  }
}
