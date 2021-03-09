/*
 * Copyright 2021-2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spanner.r2dbc.it;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.spanner.r2dbc.client.GrpcClient;
import com.google.cloud.spanner.r2dbc.util.ObservableReactiveUtil;
import com.google.protobuf.Empty;
import com.google.spanner.v1.DatabaseName;
import com.google.spanner.v1.DeleteSessionRequest;
import com.google.spanner.v1.ListSessionsRequest;
import com.google.spanner.v1.ListSessionsResponse;
import com.google.spanner.v1.Session;
import com.google.spanner.v1.SpannerGrpc.SpannerStub;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * This class does not follow test naming conventions intentionally.
 * It is intended for one-off use troubleshooting and cleaning up sessions.
 * Note that during normal operation, no leftover sessions should be present. If leftover sessions
 * are detected, it's due to a bug and should be fixed.
 */
@Disabled
public class SessionCleanupUtils {

  static GrpcClient client;
  static SpannerStub spannerStub;

  @BeforeAll
  static void setUpEnvironment() throws Exception {
    client = new GrpcClient(GoogleCredentials.getApplicationDefault());
    spannerStub = client.getSpanner();
  }

  @Test
  void deleteAllSessions() throws Exception {
    List<String> activeSessions = getSessionNames();
    System.out.println("Deleting " + activeSessions.size() + " sessions");
    int i = 0;
    for (String name : activeSessions) {
      if (i % 50 == 0) {
        // occasionally give all calls time to finish.
        Thread.sleep(200);
      }
      i++;
      spannerStub.deleteSession(
          DeleteSessionRequest.newBuilder().setName(name).build(),
          new StreamObserver<Empty>() {
            @Override
            public void onNext(Empty empty) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
              System.out.println("Deleted session " + name);
            }
          });
    }

    Thread.sleep(1000);

  }

  /** In stable state, there should be no stray sessions.
   * Exceptions:
   * - you are running in a CI environment and there are other tests running.
   * - you want to verify session state after a test. In this case, make another method
   *   verifying the correct number of sessions (exactly 1, greater than 10 etc.)
   */
  @Test
  void verifyNoLeftoverSessions() throws Exception {
    List<String> activeSessions = getSessionNames();
    assertThat(activeSessions).isEmpty();
  }

  private List<String> getSessionNames() throws Exception {
    String databaseName = DatabaseName.format(ServiceOptions.getDefaultProjectId(),
        DatabaseProperties.INSTANCE, DatabaseProperties.DATABASE);

    String nextPageToken = null;
    List<String> sessionNames = new ArrayList<>();

    do {
      ListSessionsRequest.Builder requestBuilder =
          ListSessionsRequest.newBuilder().setDatabase(databaseName);

      if (nextPageToken != null) {
        requestBuilder.setPageToken(nextPageToken);
      }

      ListSessionsResponse listSessionsResponse =
          ObservableReactiveUtil.<ListSessionsResponse>unaryCall(
              obs -> spannerStub.listSessions(requestBuilder.build(), obs))
              .block();

      nextPageToken = listSessionsResponse.getNextPageToken();
      sessionNames.addAll(
          listSessionsResponse.getSessionsList().stream()
              .map(Session::getName)
              .collect(Collectors.toList()));
    } while (nextPageToken != null && !"".equals(nextPageToken));


    return sessionNames;
  }
}
