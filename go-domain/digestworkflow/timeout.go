package digestworkflow

import (
	"context"
	"time"
)

// GenerationTimeout bounds the complete provider phase, including admission
// and existing retries. An earlier job deadline or cancellation still wins.
const GenerationTimeout = 3 * time.Minute

func generationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, GenerationTimeout)
}
