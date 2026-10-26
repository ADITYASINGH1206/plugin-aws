package io.kestra.plugin.aws.sqs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Purge all messages from an SQS queue",
    description = "Deletes all messages in the specified SQS queue URL using the PurgeQueue action. Note that AWS throttles PurgeQueue to once every 60 seconds per queue."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Purge an SQS Dead-Letter Queue before a batch ingestion run",
            code = """
                id: purge_dlq
                namespace: company.finops

                tasks:
                  - id: purge_sqs
                    type: io.kestra.plugin.aws.sqs.Purge
                    accessKeyId: "{{ secret('AWS_ACCESS_KEY_ID') }}"
                    secretKeyId: "{{ secret('AWS_SECRET_KEY_ID') }}"
                    region: "eu-west-1"
                    queueUrl: "https://sqs.eu-west-1.amazonaws.com/123456789012/my-dlq"
                """
        )
    }
)
public class Purge extends AbstractSqs implements RunnableTask<Purge.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rQueueUrl = runContext.render(getQueueUrl()).as(String.class).orElseThrow();

        try (var sqsClient = this.client(runContext)) {
            runContext.logger().info("Purging SQS queue: {}", rQueueUrl);

            sqsClient.purgeQueue(PurgeQueueRequest.builder()
                .queueUrl(rQueueUrl)
                .build());

            return Output.builder()
                .queueUrl(rQueueUrl)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The URL of the purged queue")
        private final String queueUrl;
    }
}
