#!/bin/bash
set -e

export AWS_REGION=eu-west-2
export AWS_DEFAULT_REGION=eu-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "Initializing LocalStack resources for NRF Impact Assessment Worker..."

# Create S3 bucket (input only)
echo "Creating S3 input bucket..."
aws --endpoint-url=http://localhost:4566 s3 mb s3://nrf-inputs 2>/dev/null || echo "  Bucket nrf-inputs already exists"

# Create Dead Letter Queue first
echo "Creating SQS Dead Letter Queue..."
DLQ_URL=$(aws --endpoint-url=http://localhost:4566 sqs create-queue \
  --queue-name nrf-assessment-queue-dlq \
  --output text --query 'QueueUrl' 2>/dev/null) || {
  echo "  Queue nrf-assessment-queue-dlq already exists"
  DLQ_URL="http://localhost:4566/000000000000/nrf-assessment-queue-dlq"
}

# Get DLQ ARN
DLQ_ARN=$(aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --output text --query 'Attributes.QueueArn')

echo "  DLQ ARN: $DLQ_ARN"

# Create main SQS queue with redrive policy
echo "Creating main SQS queue with DLQ redrive policy..."
REDRIVE_POLICY="{\"deadLetterTargetArn\":\"$DLQ_ARN\",\"maxReceiveCount\":\"3\"}"

aws --endpoint-url=http://localhost:4566 sqs create-queue \
  --queue-name nrf-assessment-queue \
  --attributes "{\"RedrivePolicy\":\"$(echo $REDRIVE_POLICY | sed 's/"/\\"/g')\"}" \
  2>/dev/null || {
    echo "  Queue nrf-assessment-queue already exists, updating redrive policy..."
    QUEUE_URL="http://localhost:4566/000000000000/nrf-assessment-queue"
    aws --endpoint-url=http://localhost:4566 sqs set-queue-attributes \
      --queue-url "$QUEUE_URL" \
      --attributes "{\"RedrivePolicy\":\"$(echo $REDRIVE_POLICY | sed 's/"/\\"/g')\"}"
  }

echo "✓ LocalStack initialization complete!"
echo "  S3 bucket (input): nrf-inputs"
echo "  SQS queue: nrf-assessment-queue"
echo "  SQS Dead Letter Queue: nrf-assessment-queue-dlq (maxReceiveCount: 3)"
echo ""
echo "Note: Messages failing validation or processing will retry 3 times then move to DLQ"
echo "Note: No S3 output bucket - results sent via email (stub)"
