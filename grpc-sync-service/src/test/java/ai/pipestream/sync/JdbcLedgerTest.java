package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.RuntimeSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcLedgerTest {

    @TempDir
    Path temp;

    @Test
    void sqliteSurvivesReopen() throws Exception {
        String jdbc = "jdbc:sqlite:" + temp.resolve("ledger.db").toAbsolutePath();
        try (JdbcLedger first = JdbcLedger.open(jdbc)) {
            first.putConnection(Connection.newBuilder()
                    .setConnectionId("acme")
                    .setKind(ConnectionKind.CONNECTION_KIND_CONFLUENCE)
                    .setDisplayName("Acme")
                    .setToken("secret")
                    .build(), true);
            first.upsert(Asset.newBuilder()
                    .setAssetId("confluence:acme:page:1")
                    .setSource("confluence")
                    .setConnectionId("acme")
                    .setKind("page")
                    .setNativeId("1")
                    .setTitle("Design")
                    .build());
            first.putSettings(RuntimeSettings.newBuilder()
                    .setKafkaBootstrapServers("kafka:9092")
                    .setOutput(ConnectionOutput.newBuilder()
                            .setStore("s3")
                            .setS3Bucket("knowledge"))
                    .build());
        }
        try (JdbcLedger second = JdbcLedger.open(jdbc)) {
            Connection row = second.getConnection("acme").orElseThrow();
            assertThat(row.getToken()).isEqualTo("secret");
            assertThat(second.get("confluence:acme:page:1").orElseThrow().getTitle())
                    .isEqualTo("Design");
            assertThat(second.list("confluence", "", "", false,
                    ai.pipestream.sync.v1.AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED,
                    0, "acme")).hasSize(1);
            assertThat(second.getSettings().getKafkaBootstrapServers()).isEqualTo("kafka:9092");
            assertThat(second.getSettings().getOutput().getS3Bucket()).isEqualTo("knowledge");
        }
    }

    @Test
    void ledgersOpenRespectsJdbcUrl() {
        Path file = temp.resolve("from-env.db");
        Ledger ledger = Ledgers.open(Map.of(Ledgers.ENV_JDBC_URL,
                "jdbc:sqlite:" + file.toAbsolutePath()));
        assertThat(ledger).isInstanceOf(JdbcLedger.class);
        ledger.close();
        assertThat(file).exists();
    }
}
