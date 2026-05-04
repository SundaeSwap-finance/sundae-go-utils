package syncV2Consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/rs/zerolog"
)

// skipBodyHashCfg disables gouroboros's body-hash validation when parsing
// blocks. Some Conway-era blocks with indefinite-length CBOR aux data fail
// validation due to an upstream gouroboros bug — the cbor decoder strips the
// 0xff break terminator from cbor.RawMessage extraction, so blake2b of the
// returned bytes doesn't match what the producer node hashed. The blocks are
// otherwise valid (Cardano consensus already accepted them and sundae-sync-v2
// fetched them via the verified Ouroboros protocol), so this validation is
// redundant in our pipeline and re-running it on bad-encoded blocks wedges
// the indexer pointlessly.
var skipBodyHashCfg = common.VerifyConfig{SkipBodyHashValidation: true}

type Syncer struct {
	Logger     zerolog.Logger
	Downloader Downloader
	Events     chan Message
	Group      *errgroup.Group
	ctx        context.Context // errgroup context; cancelled when sync goroutine exits with error
}

// downloadResult carries either the block bytes or the error from the async
// downloader. Using a struct (rather than a bare []byte channel) lets the
// sync goroutine surface download failures as per-event errors instead of
// blocking forever on a channel that will never receive.
type downloadResult struct {
	bytes []byte
	err   error
}

// Block is the wire shape of a single block reference in a sync-v2 broadcast
// message. The producer (Rust, sundae-sync-v2) marshals this from
// utxorpc::spec::sync::BlockRef. As of utxorpc 0.13 the JSON field name for
// the block's slot is "slot" (it was "index" in 0.11; the producer was bumped
// 2026-02-19 in commit e32ffad of sundae-sync-v2). The actual block consumer
// only uses Hash to fetch the block from S3 and reads slot/height from the
// decoded CBOR, so the rename slipped past everything except the heartbeat
// Lambda. Keep the field names matching the wire format so future readers
// don't have to bisect Rust commits to figure out what's going on.
type Block struct {
	Slot     json.Number         `json:"slot"`
	Height   json.Number         `json:"height"`
	Hash     []byte              `json:"hash"`
	Contents chan downloadResult `json:"-"`
}
type Message struct {
	Undo     []Block    `json:"undo"`
	Advance  Block      `json:"advance"`
	Finished chan error `json:"-"`
}

type UndoFunc func(ctx context.Context, tx ledger.Transaction, slot uint64) error
type AdvanceFunc func(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error

func (h *Syncer) SpawnSyncFunc(group *errgroup.Group, ctx context.Context, undoFunc UndoFunc, advanceFunc AdvanceFunc) {
	h.ctx = ctx
	group.Go(func() error {
		// Drain events forever. Per-event errors are reported via event.Finished
		// and the loop continues — a single bad block (or downloader failure)
		// must not poison the long-lived goroutine, otherwise every subsequent
		// invocation on the same warm Lambda container fails instantly with
		// "sync goroutine no longer running".
		for event := range h.Events {
			err := h.processEvent(ctx, event, undoFunc, advanceFunc)
			event.Finished <- err
		}
		return nil
	})
}

// processEvent handles one Message: drains downloader results, decodes blocks,
// and dispatches transactions through undo/advance. All errors are returned;
// the caller decides whether to keep the goroutine alive.
func (h *Syncer) processEvent(ctx context.Context, event Message, undoFunc UndoFunc, advanceFunc AdvanceFunc) (err error) {
	defer func() {
		if panicCause := recover(); panicCause != nil {
			h.Logger.Error().Any("panicCause", panicCause).Msg("panic while processing blocks")
			err = fmt.Errorf("panic while processing blocks: %v", panicCause)
		}
	}()

	for _, undo := range event.Undo {
		res := <-undo.Contents
		if res.err != nil {
			h.Logger.Warn().Str("blockHash", hex.EncodeToString(undo.Hash)).Err(res.err).Msg("Failed downloading undo block")
			return fmt.Errorf("download undo block %s: %w", hex.EncodeToString(undo.Hash), res.err)
		}
		blockType := uint(res.bytes[1])
		block, err := ledger.NewBlockFromCbor(blockType, res.bytes[2:], skipBodyHashCfg)
		if err != nil {
			h.Logger.Warn().Str("blockHash", hex.EncodeToString(undo.Hash)).Err(err).Msg("Error decoding block for undo")
			return err
		}
		txs := block.Transactions()
		slices.Reverse(txs)
		for _, tx := range txs {
			if err := undoFunc(ctx, tx, block.SlotNumber()); err != nil {
				h.Logger.Warn().Str("blockHash", hex.EncodeToString(undo.Hash)).Err(err).Msg("Error executing undo logic for transaction")
				return err
			}
		}
	}

	res := <-event.Advance.Contents
	if res.err != nil {
		h.Logger.Warn().Str("blockHash", hex.EncodeToString(event.Advance.Hash)).Err(res.err).Msg("Failed downloading advance block")
		return fmt.Errorf("download advance block %s: %w", hex.EncodeToString(event.Advance.Hash), res.err)
	}
	blockType := uint(res.bytes[1])
	block, err := ledger.NewBlockFromCbor(blockType, res.bytes[2:], skipBodyHashCfg)
	if err != nil {
		h.Logger.Warn().Str("blockHash", hex.EncodeToString(event.Advance.Hash)).Err(err).Msg("Error decoding block for advance")
		return err
	}
	for index, tx := range block.Transactions() {
		if err := advanceFunc(ctx, tx, block.SlotNumber(), index); err != nil {
			h.Logger.Warn().Str("blockHash", hex.EncodeToString(event.Advance.Hash)).Err(err).Msg("Error executing advance logic for transaction")
			return err
		}
	}
	return nil
}

func (h *Syncer) HandleOne(data []byte) chan error {
	finished := make(chan error, 1)
	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		finished <- err
		return finished
	}
	for idx := range message.Undo {
		message.Undo[idx].Contents = make(chan downloadResult, 1)
	}
	message.Advance.Contents = make(chan downloadResult, 1)
	message.Finished = finished

	// Plain goroutines (not h.Group.Go) so a downloader failure does NOT cancel
	// the errgroup context — that would kill the long-lived sync goroutine and
	// poison every subsequent Lambda invocation on this warm container. The
	// download error is delivered to processEvent via the Contents channel and
	// surfaced as a per-event error instead.
	for _, undo := range message.Undo {
		go func(b Block) {
			bytes, err := h.Downloader.DownloadBlockSync(b.Hash)
			b.Contents <- downloadResult{bytes: bytes, err: err}
		}(undo)
	}
	go func(b Block) {
		bytes, err := h.Downloader.DownloadBlockSync(b.Hash)
		b.Contents <- downloadResult{bytes: bytes, err: err}
	}(message.Advance)

	h.Events <- message
	return finished
}
