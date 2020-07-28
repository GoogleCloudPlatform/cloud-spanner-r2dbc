package com.example;

import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.DRIVER_NAME;
import static com.google.cloud.spanner.r2dbc.SpannerConnectionFactoryProvider.INSTANCE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/artworks")
public class MetController {

  static final String TEST_INSTANCE
      = System.getProperty("spanner.instance", "reactivetest");
  static final String TEST_DATABASE
      = System.getProperty("spanner.database", "met");
  static final String TEST_PROJECT
      = System.getProperty("gcp.project", "elfel-spring");

  ConnectionFactory connectionFactoryGrpc = ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
          .option(Option.valueOf("project"), TEST_PROJECT)
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, TEST_INSTANCE)
          .option(DATABASE, TEST_DATABASE)
          .build());

  ConnectionFactory connectionFactoryClientLibrary = ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
          .option(Option.valueOf("project"), TEST_PROJECT)
          .option(DRIVER, DRIVER_NAME)
          .option(INSTANCE, TEST_INSTANCE)
          .option(DATABASE, TEST_DATABASE)
          .option(Option.valueOf("client-implementation"), "client-library")
          .build());

  SpannerOptions options = SpannerOptions.newBuilder().build();
  Spanner spanner = options.getService();

  DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
      options.getProjectId(), TEST_INSTANCE, TEST_DATABASE));

  private String generateQuery() {
    StringBuilder sb = new StringBuilder("SELECT * FROM met_objects WHERE object_id IN (");
    for (int i = 0; i < 9; i++) {
      sb.append(getRandomObjectId());
      sb.append(", ");
    }
    sb.append(getRandomObjectId());
    sb.append(")");
    return sb.toString();
  }


  final Integer[] objectIds = new Integer[] {
      1010,2007,4001,5995,6992,7989,9983,10980,11977,14968,15965,16962,
      18956,21947,22944,23941,26932,29923,36902,37899,38896,39893,40890,41887,42884,43881,44878,
      45875,46872,47869,49863,50860,53851,54848,55845,56842,58836,59833,60830,63821,65815,66812,
      67809,68806,69803,81767,83761,86752,102704,104698,106692,108686,112674,122644,157539,168506,
      169503,173491,186452,187449,188446,189443,191437,193431,194428,195425,196422,197419,198416,
      199413,200410,201407,202404,203401,205395,206392,207389,208386,209383,211377,213371,214368,
      216362,219353,221347,223341,224338,226332,227329,228326,229323,230320,241287,242284,243281,
      244278,245275,247269,248266,249263,250260,251257,253251,256242,257239,263221,268206,270200,
      282164,283161,286152,287149,288146,308086,309083,313071,315065,317059,319053,320050,322044,
      324038,325035,326032,327029,334008,335005,336002,336999,337996,338993,339990,341984,342981,
      347966,348963,358933,361924,362921,365912,368903,369900,372891,373888,375882,376879,377876,
      381864,386849,389840,391834,392831,394825,397816,398813,399810,400807,402801,406789,407786,
      411774,414765,429720,434705,435702,436699,437696,443678,445672,446669,447666,448663,451654,
      452651,453648,454645,457636,459630,461624,464615,465612,467606,474585,482561,483558,484555,
      485552,488543,501504,546369,548363,549360,551354,552351,554345,557336,558333,560327,575282,
      632111};
  final Random random = new Random();

  Integer getRandomObjectId() {
    return this.objectIds[this.random.nextInt(this.objectIds.length)];
  }

  Integer getRandomValue() {
    return this.random.nextInt(1000);
  }

  // TODO: since result set is async, try turning it into a flux
  @GetMapping("/client-library")
  Flux<String> getArtworksClientLibrary() {
    ResultSet resultSet = this.dbClient.singleUse()
        .executeQuery(Statement.of(generateQuery()));
    List<String> titles = new ArrayList<>();

    return Flux.create(sink -> {
      while (resultSet.next()) {
        sink.next(resultSet.getCurrentRowAsStruct().getString("title") + "\n<br/>");
      }
      sink.complete();
    });

  }

  @GetMapping("/r2dbc-grpc")
  Flux<String> getArtworksR2dbcGrpc() {
    return Flux.from(this.connectionFactoryGrpc.create())
        .flatMap(conn -> conn.createStatement(generateQuery()).execute())
            .flatMap(spannerResult -> spannerResult.map(
                (r, meta) -> r.get("title", String.class) + "\n<br/>"
            ));
  }

  @GetMapping("/r2dbc-clientlibrary")
  Flux<String> getArtworksR2dbcClientLibrary() {
    return Mono.from(this.connectionFactoryClientLibrary.create())
        .flatMapMany(conn -> conn.createStatement(generateQuery()).execute())
        .flatMap(spannerResult -> spannerResult.map(
            (r, meta) -> {
              System.out.println("Got ROW: " + r);
              return r.get("title", String.class) + "\n<br/>";
            }
        ));

    /*
    *  Mono.from(conn.createStatement("SELECT count(*) FROM BOOKS").execute())
          .flatMapMany(rs -> rs.map((row, rmeta) -> (Long)row.get(1)))*/
  }

}
