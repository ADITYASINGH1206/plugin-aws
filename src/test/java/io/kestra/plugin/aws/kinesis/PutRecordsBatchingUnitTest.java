package io.kestra.plugin.aws.kinesis;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.aws.kinesis.model.Record;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class PutRecordsBatchingUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    private List<PutRecordsResultEntry> generateResultEntries(int count) {
        var list = new ArrayList<PutRecordsResultEntry>();
        for (var i = 0; i < count; i++) {
            list.add(
                PutRecordsResultEntry.builder()
                    .sequenceNumber("seq-" + i)
                    .shardId("shard-000000000000")
                    .build()
            );
        }
        return list;
    }

    @Test
    void testBatchingRequestShapeAndFailureCount() throws Exception {
        var runContext = runContextFactory.of();

        var mockClient = mock(KinesisClient.class);
        when(mockClient.putRecords(any(PutRecordsRequest.class)))
            .thenReturn(
                PutRecordsResponse.builder()
                    .records(generateResultEntries(500))
                    .failedRecordCount(1)
                    .build(),
                PutRecordsResponse.builder()
                    .records(generateResultEntries(500))
                    .failedRecordCount(0)
                    .build(),
                PutRecordsResponse.builder()
                    .records(generateResultEntries(50))
                    .failedRecordCount(0)
                    .build()
            );

        var records = new ArrayList<Record>();
        for (var i = 0; i < 1050; i++) {
            records.add(
                Record.builder()
                    .partitionKey("pk-" + i)
                    .data("payload-" + i)
                    .build()
            );
        }

        var task = PutRecords.builder()
            .id("unit-test-put-records")
            .type(PutRecords.class.getName())
            .records(records)
            .streamName(Property.ofValue("test-stream"))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .failOnUnsuccessfulRecords(Property.ofValue(false))
            .build();

        var spyTask = spy(task);
        doReturn(mockClient).when(spyTask).client(any());

        var output = spyTask.run(runContext);

        var captor = ArgumentCaptor.forClass(PutRecordsRequest.class);
        verify(mockClient, times(3)).putRecords(captor.capture());

        var capturedRequests = captor.getAllValues();
        assertEquals(500, capturedRequests.get(0).records().size());
        assertEquals(500, capturedRequests.get(1).records().size());
        assertEquals(50, capturedRequests.get(2).records().size());

        assertEquals(1, output.getFailedRecordsCount());
        assertEquals(1050, output.getRecordCount());
    }

    @Test
    void testBatchingFailsWhenConfigured() throws Exception {
        var runContext = runContextFactory.of();

        var mockClient = mock(KinesisClient.class);
        when(mockClient.putRecords(any(PutRecordsRequest.class)))
            .thenReturn(
                PutRecordsResponse.builder()
                    .records(generateResultEntries(500))
                    .failedRecordCount(2)
                    .build()
            );

        var records = new ArrayList<Record>();
        for (var i = 0; i < 500; i++) {
            records.add(
                Record.builder()
                    .partitionKey("pk-" + i)
                    .data("payload-" + i)
                    .build()
            );
        }

        var task = PutRecords.builder()
            .id("unit-test-put-records")
            .type(PutRecords.class.getName())
            .records(records)
            .streamName(Property.ofValue("test-stream"))
            .region(Property.ofValue("us-east-1"))
            .accessKeyId(Property.ofValue("test-key"))
            .secretKeyId(Property.ofValue("test-secret"))
            .failOnUnsuccessfulRecords(Property.ofValue(true))
            .build();

        var spyTask = spy(task);
        doReturn(mockClient).when(spyTask).client(any());

        assertThrows(RuntimeException.class, () -> spyTask.run(runContext));
    }
}
