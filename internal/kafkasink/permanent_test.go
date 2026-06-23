package kafkasink

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// TestPermanentClassification guards the franz-go kerr → permanent/transient
// mapping against drift: a misclassified error either wedges the sink on an
// unretryable failure (treated transient) or fails fast on a retryable one
// (losslessness regression).
func TestPermanentClassification(t *testing.T) {
	permanentErrs := []*kerr.Error{
		kerr.MessageTooLarge,
		kerr.RecordListTooLarge,
		kerr.UnknownTopicOrPartition,
		kerr.TopicAuthorizationFailed,
		kerr.ClusterAuthorizationFailed,
		kerr.SaslAuthenticationFailed,
		kerr.InvalidTopicException,
		kerr.InvalidRecord,
		kerr.UnsupportedForMessageFormat,
		kerr.UnsupportedVersion,
	}
	for _, e := range permanentErrs {
		if !permanent(e) {
			t.Errorf("%v should be classified permanent", e)
		}
		// Wrapping must not lose the classification (the loop uses errors.As).
		if !permanent(fmt.Errorf("produce failed: %w", e)) {
			t.Errorf("wrapped %v should still be permanent", e)
		}
	}

	transientErrs := []error{
		kerr.NotLeaderForPartition,                 // retryable: leadership moved
		kerr.RequestTimedOut,                       // retryable: timeout
		kerr.NotEnoughReplicas,                     // retryable: ISR shrank
		kerr.CoordinatorLoadInProgress,             // retryable: transient broker state
		errors.New("dial tcp: connection refused"), // transport error (not a kerr)
		nil,
	}
	for _, e := range transientErrs {
		if permanent(e) {
			t.Errorf("%v should be classified transient (retryable)", e)
		}
	}
}
