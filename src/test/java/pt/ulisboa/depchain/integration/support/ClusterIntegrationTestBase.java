package pt.ulisboa.depchain.integration.support;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ClusterIntegrationTestBase extends IntegrationHarness {
  private final Map<String, String> previousSystemProperties = new LinkedHashMap<>();
  private ManagedCluster sharedCluster;

  @BeforeAll
  final void startSharedCluster() throws Exception {
    Map<String, String> jvmProperties = clusterJvmProperties();
    for (Map.Entry<String, String> entry : jvmProperties.entrySet()) {
      previousSystemProperties.put(entry.getKey(), System.getProperty(entry.getKey()));
      System.setProperty(entry.getKey(), entry.getValue());
    }

    try {
      sharedCluster = startManagedCluster(replicaIds(), jvmProperties);
      afterClusterStarted(sharedCluster);
    } catch (Exception exception) {
      restoreSystemProperties();
      throw exception;
    }
  }

  @AfterAll
  final void stopSharedCluster() throws Exception {
    try {
      if (sharedCluster != null) {
        sharedCluster.close();
      }
    } finally {
      restoreSystemProperties();
    }
  }

  protected List<String> replicaIds() {
    return REPLICA_IDS;
  }

  protected Map<String, String> clusterJvmProperties() {
    return Map.of();
  }

  protected void afterClusterStarted(ManagedCluster cluster) throws Exception {
  }

  protected final ManagedCluster cluster() {
    if (sharedCluster == null) {
      throw new IllegalStateException("Shared cluster has not been started");
    }
    return sharedCluster;
  }

  private void restoreSystemProperties() {
    for (Map.Entry<String, String> entry : previousSystemProperties.entrySet()) {
      if (entry.getValue() == null) {
        System.clearProperty(entry.getKey());
      } else {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
    previousSystemProperties.clear();
  }
}
