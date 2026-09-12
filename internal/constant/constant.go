// Package constant holds the gateway's wire vocabulary: topic names, event
// types, and header values it consumes and produces.
package constant

import "fmt"

// Topics on the OCR pipeline (source of truth: docs/reference/api-ocr.md and
// api-ocr-gateway.md).
const (
	// TopicDocumentProcessed is produced by ocr's outbox and consumed by this
	// service - the only topic the gateway subscribes to.
	TopicDocumentProcessed = "ocr.document.processed.v1"
	// TopicDocumentProcessedDLQ receives records the consumer cannot route:
	// contract violations and events for unregistered callers.
	TopicDocumentProcessedDLQ = "ocr.document.processed.dlq"
)

// RoutedTopic renders the per-caller routed topic from the template. The
// template lives in config (KAFKA_ROUTED_TOPIC_TEMPLATE) so a deployment can
// rename the family without a code change; the shape is
// ocr.document.routed.<service>.v1.
func RoutedTopic(template, callerID string) string {
	return fmt.Sprintf(template, callerID)
}

// Event types and producer identity stamped on routed events.
const (
	// EventTypeDocumentProcessed is what ocr emits (the input envelope).
	EventTypeDocumentProcessed = "document.processed"
	// EventTypeDocumentRouted is what the gateway emits (the output envelope).
	EventTypeDocumentRouted = "document.routed"
	// ProducerName identifies the gateway on every routed event.
	ProducerName = "ocr-gateway"
	// SourceService is the source-service header on published records.
	SourceService = "ocr-gateway"
)

// Kafka header names (docs/reference/kafka-contract.md shape).
const (
	HeaderEventID      = "event-id"
	HeaderEventType    = "event-type"
	HeaderEventVersion = "event-version"
	HeaderSource       = "source-service"
	HeaderAggType      = "aggregate-type"
	HeaderAggID        = "aggregate-id"
	HeaderTraceID      = "trace-id"
)

// AggregateType is the only aggregate on this pipeline.
const AggregateType = "document"

// EventVersion is the routed event's version.
const EventVersion = 1
