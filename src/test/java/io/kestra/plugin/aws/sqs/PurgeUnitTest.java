package io.kestra.plugin.aws.sqs;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.PurgeQueueInProgressException;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@KestraTest
class PurgeUnitTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    void shouldPurgeQueueSuccessfully() throws Exception {
        var runContext = runContextFactory.of();
        var queueUrl = "https://sqs.us-east-1.amazonaws.com/000000000000/test-queue";

        SqsClient mockClient = mock(SqsClient.class);

        Purge task = spy(
            Purge.builder()
                .id("test-purge")
                .type(Purge.class.getName())
                .queueUrl(Property.ofValue(queueUrl))
                .region(Property.ofValue("us-east-1"))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        Purge.Output output = task.run(runContext);

        assertThat(output).isNotNull();
        assertThat(output.getQueueUrl()).isEqualTo(queueUrl);

        ArgumentCaptor<PurgeQueueRequest> captor = ArgumentCaptor.forClass(PurgeQueueRequest.class);
        verify(mockClient).purgeQueue(captor.capture());
        assertThat(captor.getValue().queueUrl()).isEqualTo(queueUrl);
    }

    @Test
    void shouldPropagatePurgeQueueInProgressException() throws Exception {
        var runContext = runContextFactory.of();
        var queueUrl = "https://sqs.us-east-1.amazonaws.com/000000000000/test-queue";

        SqsClient mockClient = mock(SqsClient.class);
        doThrow(PurgeQueueInProgressException.builder().message("Purge in progress").build())
            .when(mockClient).purgeQueue(any(PurgeQueueRequest.class));

        Purge task = spy(
            Purge.builder()
                .id("test-purge-throttled")
                .type(Purge.class.getName())
                .queueUrl(Property.ofValue(queueUrl))
                .region(Property.ofValue("us-east-1"))
                .build()
        );

        doReturn(mockClient).when(task).client(any());

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(PurgeQueueInProgressException.class)
            .hasMessageContaining("Purge in progress");
    }
}
