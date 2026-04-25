package replay

// This file implements DDBCoordinator on top of two DynamoDB tables:
//
//   - {prefix}-claims: one row per chunk. pk = chunk_idx (number, as string).
//     Holds the height range, status, current worker_id, and lease expiry.
//     Plus one special row pk = "cursor" with attributes total (uint) and
//     next_idx (uint), used as an atomic counter for fresh chunk handout.
//   - {prefix}-tx: one row per processed transaction. pk = tx_hash. Used by
//     FindTx for cross-chunk dependency resolution.
//
// The setup script seeds the claims table with one row per chunk and the
// cursor row. Workers use AtomicCounter-style updates on the cursor to grab
// fresh chunks, and fall back to scanning for expired leases once the fresh
// queue is exhausted (worker crash recovery).
//
// Tables are intended to be **throwaway**: created per replay, dropped after.
// Schema and setup live in setup_ddb.go.

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
)

const (
	chunkStatusPending    = "pending"
	chunkStatusInProgress = "in_progress"
	chunkStatusDone       = "done"

	cursorRowID = "cursor"
)

// DDBCoordinator implements Coordinator using two DDB tables.
type DDBCoordinator struct {
	api         dynamodbiface.DynamoDBAPI
	claimsTable string
	txTable     string
	workerID    string
	leaseTTL    time.Duration
	logger      zerolog.Logger

	// totalChunks is set by ClaimChunk on first call (read from the cursor row).
	totalOnce   sync.Once
	totalChunks uint64
	totalErr    error

	// localTxCache short-circuits FindTx for txs we just published, so a worker
	// processing a chunk doesn't repeatedly hit DDB for txs it just wrote.
	cacheMu       sync.RWMutex
	publishedTxs  map[string]uint64
	publishedSize int // approximate; reset when too large

	// idxByStart caches chunk_idx by start-height so CompleteChunk and
	// Heartbeat don't need a full table scan to find their chunk.
	idxCacheMu sync.RWMutex
	idxByStart map[uint64]uint64
}

// NewDDBCoordinator returns a Coordinator backed by two DDB tables. Caller
// is responsible for creating and bootstrapping the tables (see SetupDDB).
func NewDDBCoordinator(api dynamodbiface.DynamoDBAPI, prefix, workerID string, leaseTTL time.Duration, logger zerolog.Logger) *DDBCoordinator {
	if leaseTTL <= 0 {
		leaseTTL = LeaseTTL
	}
	return &DDBCoordinator{
		api:          api,
		claimsTable:  prefix + "-claims",
		txTable:      prefix + "-tx",
		workerID:     workerID,
		leaseTTL:     leaseTTL,
		logger:       logger,
		publishedTxs: make(map[string]uint64),
		idxByStart:   make(map[uint64]uint64),
	}
}

// loadTotalChunks reads the cursor row's "total" attribute (set by SetupDDB)
// once, on first ClaimChunk. Lets workers know when to stop trying fresh
// chunks and switch to scanning for expired leases.
func (c *DDBCoordinator) loadTotalChunks(ctx context.Context) (uint64, error) {
	c.totalOnce.Do(func() {
		out, err := c.api.GetItemWithContext(ctx, &dynamodb.GetItemInput{
			TableName:      aws.String(c.claimsTable),
			Key:            map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(cursorRowID)}},
			ConsistentRead: aws.Bool(true),
		})
		if err != nil {
			c.totalErr = fmt.Errorf("read cursor row: %w", err)
			return
		}
		if out.Item == nil {
			c.totalErr = fmt.Errorf("cursor row missing — was setup-ddb run for prefix %s?", c.claimsTable)
			return
		}
		v, ok := out.Item["total"]
		if !ok || v.N == nil {
			c.totalErr = fmt.Errorf("cursor row missing 'total' attribute")
			return
		}
		n, err := strconv.ParseUint(*v.N, 10, 64)
		if err != nil {
			c.totalErr = fmt.Errorf("parse total: %w", err)
			return
		}
		c.totalChunks = n
	})
	return c.totalChunks, c.totalErr
}

// ClaimChunk grabs an unprocessed chunk, atomically.
//
// Strategy:
//  1. Atomically increment cursor.next_idx (ADD :1) and read back the OLD value.
//     The OLD value is our fresh chunk index. This is contention-free for
//     fresh chunks — each worker gets a unique index without a CAS retry loop.
//  2. If our index < totalChunks, conditionally update that chunk row to
//     status=in_progress with our worker_id + lease. Return its range.
//  3. If our index >= totalChunks, fall back to scanning for chunks whose
//     lease has expired (worker crash recovery). Try to claim one.
//  4. If neither yields a chunk, return ok=false.
func (c *DDBCoordinator) ClaimChunk(ctx context.Context) (uint64, uint64, bool, error) {
	total, err := c.loadTotalChunks(ctx)
	if err != nil {
		return 0, 0, false, err
	}

	// Step 1: bump the cursor, get our fresh index.
	out, err := c.api.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String(c.claimsTable),
		Key:              map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(cursorRowID)}},
		UpdateExpression: aws.String("ADD next_idx :one"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":one": {N: aws.String("1")},
		},
		ReturnValues: aws.String(dynamodb.ReturnValueUpdatedOld),
	})
	if err != nil {
		return 0, 0, false, fmt.Errorf("bump cursor: %w", err)
	}

	var idx uint64
	if old := out.Attributes["next_idx"]; old != nil && old.N != nil {
		idx, _ = strconv.ParseUint(*old.N, 10, 64)
	}

	// Step 2: fresh chunk available?
	if idx < total {
		start, end, ok, err := c.takeChunk(ctx, idx, true)
		if err != nil || ok {
			return start, end, ok, err
		}
		// Falls through to rescue scan if takeChunk somehow fails CAS
		// (rare — could happen on retry). Try the rescue path.
	}

	// Step 3: rescue an expired lease.
	idx, start, end, ok, err := c.findExpiredChunk(ctx)
	if err != nil || !ok {
		return 0, 0, ok, err
	}
	start2, end2, claimed, err := c.takeChunk(ctx, idx, false)
	if err != nil || !claimed {
		// Lost the race; not fatal — caller will just call ClaimChunk again.
		return 0, 0, false, err
	}
	_, _ = start, end
	return start2, end2, true, nil
}

// takeChunk performs the conditional update to claim chunk #idx. fresh=true
// means we got the index from the cursor (status should be pending and
// uncontested); fresh=false means we're rescuing an expired lease.
func (c *DDBCoordinator) takeChunk(ctx context.Context, idx uint64, fresh bool) (uint64, uint64, bool, error) {
	now := time.Now().Unix()
	leaseUntil := now + int64(c.leaseTTL.Seconds())

	// Build only the values referenced by the chosen condition. DDB rejects
	// requests with ExpressionAttributeValues entries that aren't used in any
	// expression (UpdateExpression or ConditionExpression).
	values := map[string]*dynamodb.AttributeValue{
		":inprog": {S: aws.String(chunkStatusInProgress)},
		":wid":    {S: aws.String(c.workerID)},
		":lease":  {N: aws.String(strconv.FormatInt(leaseUntil, 10))},
	}
	var cond string
	if fresh {
		cond = "attribute_exists(pk) AND #st = :pending AND attribute_not_exists(worker_id)"
		values[":pending"] = &dynamodb.AttributeValue{S: aws.String(chunkStatusPending)}
	} else {
		cond = "attribute_exists(pk) AND #st <> :done AND leased_until < :now"
		values[":done"] = &dynamodb.AttributeValue{S: aws.String(chunkStatusDone)}
		values[":now"] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(now, 10))}
	}

	out, err := c.api.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(c.claimsTable),
		Key: map[string]*dynamodb.AttributeValue{
			"pk": {S: aws.String(strconv.FormatUint(idx, 10))},
		},
		UpdateExpression:    aws.String("SET #st = :inprog, worker_id = :wid, leased_until = :lease"),
		ConditionExpression: aws.String(cond),
		ExpressionAttributeNames: map[string]*string{
			"#st": aws.String("status"),
		},
		ExpressionAttributeValues: values,
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	})
	if err != nil {
		var aerr awserr.Error
		if e, ok := err.(awserr.Error); ok {
			aerr = e
		}
		if aerr != nil && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("claim chunk %d: %w", idx, err)
	}

	start, end, perr := chunkRangeFromAttrs(out.Attributes)
	if perr != nil {
		return 0, 0, false, fmt.Errorf("chunk %d: %w", idx, perr)
	}
	c.idxCacheMu.Lock()
	c.idxByStart[start] = idx
	c.idxCacheMu.Unlock()
	c.logger.Debug().Uint64("chunk_idx", idx).Uint64("start", start).Uint64("end", end).Msg("claimed chunk")
	return start, end, true, nil
}

// findExpiredChunk scans the claims table for a chunk whose lease has
// expired and which is not yet done. Returns the first match (or ok=false).
// Cost scales with the table size, so this is only the rescue path.
func (c *DDBCoordinator) findExpiredChunk(ctx context.Context) (uint64, uint64, uint64, bool, error) {
	now := time.Now().Unix()

	in := &dynamodb.ScanInput{
		TableName: aws.String(c.claimsTable),
		FilterExpression: aws.String(
			"attribute_exists(leased_until) AND leased_until < :now AND #st <> :done",
		),
		ExpressionAttributeNames: map[string]*string{
			"#st": aws.String("status"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":now":  {N: aws.String(strconv.FormatInt(now, 10))},
			":done": {S: aws.String(chunkStatusDone)},
		},
		Limit: aws.Int64(50),
	}

	for {
		out, err := c.api.ScanWithContext(ctx, in)
		if err != nil {
			return 0, 0, 0, false, fmt.Errorf("scan for expired chunks: %w", err)
		}
		for _, item := range out.Items {
			if pk := item["pk"]; pk != nil && pk.S != nil && *pk.S != cursorRowID {
				idx, perr := strconv.ParseUint(*pk.S, 10, 64)
				if perr != nil {
					continue
				}
				start, end, perr := chunkRangeFromAttrs(item)
				if perr != nil {
					continue
				}
				return idx, start, end, true, nil
			}
		}
		if out.LastEvaluatedKey == nil {
			return 0, 0, 0, false, nil
		}
		in.ExclusiveStartKey = out.LastEvaluatedKey
	}
}

// CompleteChunk marks the chunk done. Idempotent.
func (c *DDBCoordinator) CompleteChunk(ctx context.Context, start, end uint64) error {
	idx, err := c.lookupChunkIdx(ctx, start)
	if err != nil {
		return err
	}
	_, err = c.api.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(c.claimsTable),
		Key:       map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(strconv.FormatUint(idx, 10))}},
		UpdateExpression: aws.String("SET #st = :done REMOVE leased_until"),
		ExpressionAttributeNames: map[string]*string{
			"#st": aws.String("status"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":done": {S: aws.String(chunkStatusDone)},
		},
	})
	if err != nil {
		return fmt.Errorf("complete chunk %d: %w", idx, err)
	}
	return nil
}

// Heartbeat extends the lease on a chunk we currently hold.
func (c *DDBCoordinator) Heartbeat(ctx context.Context, start, end uint64) error {
	idx, err := c.lookupChunkIdx(ctx, start)
	if err != nil {
		return err
	}
	leaseUntil := time.Now().Unix() + int64(c.leaseTTL.Seconds())
	_, err = c.api.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(c.claimsTable),
		Key:       map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(strconv.FormatUint(idx, 10))}},
		UpdateExpression: aws.String("SET leased_until = :lease"),
		ConditionExpression: aws.String("worker_id = :wid"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":lease": {N: aws.String(strconv.FormatInt(leaseUntil, 10))},
			":wid":   {S: aws.String(c.workerID)},
		},
	})
	if err != nil {
		return fmt.Errorf("heartbeat chunk %d: %w", idx, err)
	}
	return nil
}

// PublishTx writes the tx → height mapping. Cached locally so repeated
// writes for the same tx (rare, but possible on retries) don't hit DDB.
func (c *DDBCoordinator) PublishTx(ctx context.Context, txHash string, height uint64) error {
	c.cacheMu.RLock()
	_, dup := c.publishedTxs[txHash]
	c.cacheMu.RUnlock()
	if dup {
		return nil
	}

	_, err := c.api.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(c.txTable),
		Item: map[string]*dynamodb.AttributeValue{
			"pk":     {S: aws.String(txHash)},
			"height": {N: aws.String(strconv.FormatUint(height, 10))},
		},
	})
	if err != nil {
		return fmt.Errorf("publish tx %s: %w", txHash, err)
	}

	c.cacheMu.Lock()
	c.publishedTxs[txHash] = height
	c.publishedSize++
	if c.publishedSize > 100000 {
		// Bound the cache. Cold callers fall back to DDB lookup, which is fine.
		c.publishedTxs = make(map[string]uint64)
		c.publishedSize = 0
	}
	c.cacheMu.Unlock()
	return nil
}

// FindTx checks the local cache first, then DDB.
func (c *DDBCoordinator) FindTx(ctx context.Context, txHash string) (uint64, bool, error) {
	c.cacheMu.RLock()
	h, ok := c.publishedTxs[txHash]
	c.cacheMu.RUnlock()
	if ok {
		return h, true, nil
	}

	out, err := c.api.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(c.txTable),
		Key:       map[string]*dynamodb.AttributeValue{"pk": {S: aws.String(txHash)}},
	})
	if err != nil {
		return 0, false, fmt.Errorf("find tx %s: %w", txHash, err)
	}
	if out.Item == nil {
		return 0, false, nil
	}
	if hv := out.Item["height"]; hv != nil && hv.N != nil {
		h, _ := strconv.ParseUint(*hv.N, 10, 64)
		return h, true, nil
	}
	return 0, true, nil // present but no height — treat as found
}

// GlobalWatermark scans the claims table for the largest contiguous done
// prefix. Slow if called frequently; the Replayer caches it between calls.
//
// Returns the highest height H such that all chunks ending at or before H+1
// are status=done.
func (c *DDBCoordinator) GlobalWatermark(ctx context.Context) (uint64, error) {
	total, err := c.loadTotalChunks(ctx)
	if err != nil {
		return 0, err
	}

	// Walk chunks in order until we find the first not-done. Cheap to start
	// with BatchGetItem in groups of 100.
	const batchSize = 100
	var watermark uint64
	for base := uint64(0); base < total; base += batchSize {
		end := base + batchSize
		if end > total {
			end = total
		}
		keys := make([]map[string]*dynamodb.AttributeValue, 0, end-base)
		for i := base; i < end; i++ {
			keys = append(keys, map[string]*dynamodb.AttributeValue{
				"pk": {S: aws.String(strconv.FormatUint(i, 10))},
			})
		}
		out, err := c.api.BatchGetItemWithContext(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				c.claimsTable: {Keys: keys},
			},
		})
		if err != nil {
			return watermark, fmt.Errorf("watermark batch get: %w", err)
		}
		// Index by chunk id so we can iterate in order.
		got := make(map[uint64]map[string]*dynamodb.AttributeValue, len(out.Responses[c.claimsTable]))
		for _, item := range out.Responses[c.claimsTable] {
			if pk := item["pk"]; pk != nil && pk.S != nil {
				idx, _ := strconv.ParseUint(*pk.S, 10, 64)
				got[idx] = item
			}
		}
		for i := base; i < end; i++ {
			item, ok := got[i]
			if !ok {
				return watermark, nil
			}
			st := ""
			if v := item["status"]; v != nil && v.S != nil {
				st = *v.S
			}
			if st != chunkStatusDone {
				return watermark, nil
			}
			endHeight, _ := strconv.ParseUint(aws.StringValue(item["end"].N), 10, 64)
			if endHeight > 0 {
				watermark = endHeight - 1
			}
		}
	}
	return watermark, nil
}

func (c *DDBCoordinator) Close() error {
	return nil
}

// chunkRangeFromAttrs reads start/end from a chunk row's attributes.
func chunkRangeFromAttrs(attrs map[string]*dynamodb.AttributeValue) (uint64, uint64, error) {
	startV := attrs["start"]
	endV := attrs["end"]
	if startV == nil || startV.N == nil || endV == nil || endV.N == nil {
		return 0, 0, fmt.Errorf("chunk missing start/end")
	}
	start, err := strconv.ParseUint(*startV.N, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	end, err := strconv.ParseUint(*endV.N, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// lookupChunkIdx finds the chunk_idx for a given start height. Workers know
// their start/end from ClaimChunk; this is a backwards-lookup helper.
//
// We could thread chunk_idx through the API instead, but keeping the public
// Coordinator surface minimal felt cleaner. The lookup is one GetItem on
// a small table, so cost is negligible.
func (c *DDBCoordinator) lookupChunkIdx(ctx context.Context, start uint64) (uint64, error) {
	total, err := c.loadTotalChunks(ctx)
	if err != nil {
		return 0, err
	}
	// Chunk start = startBase + idx*chunkSize, but we don't have those here.
	// Easier: scan-by-start (table is small, < few thousand chunks usually).
	// In practice we keep an in-memory map populated by ClaimChunk.
	c.idxCacheMu.RLock()
	idx, ok := c.idxByStart[start]
	c.idxCacheMu.RUnlock()
	if ok {
		return idx, nil
	}
	// Fallback: linear search via batch gets. Should rarely happen.
	for base := uint64(0); base < total; base += 100 {
		end := base + 100
		if end > total {
			end = total
		}
		keys := make([]map[string]*dynamodb.AttributeValue, 0, end-base)
		for i := base; i < end; i++ {
			keys = append(keys, map[string]*dynamodb.AttributeValue{
				"pk": {S: aws.String(strconv.FormatUint(i, 10))},
			})
		}
		out, err := c.api.BatchGetItemWithContext(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				c.claimsTable: {Keys: keys},
			},
		})
		if err != nil {
			return 0, fmt.Errorf("lookup chunk idx scan: %w", err)
		}
		for _, item := range out.Responses[c.claimsTable] {
			startVal := item["start"]
			if startVal == nil || startVal.N == nil {
				continue
			}
			s, _ := strconv.ParseUint(*startVal.N, 10, 64)
			if s == start {
				if pk := item["pk"]; pk != nil && pk.S != nil {
					idx, _ := strconv.ParseUint(*pk.S, 10, 64)
					return idx, nil
				}
			}
		}
	}
	return 0, fmt.Errorf("no chunk found with start=%d", start)
}
