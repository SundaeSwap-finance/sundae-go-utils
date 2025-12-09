package syncV2Consumer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/rs/zerolog"
)

type Syncer struct {
	Logger     zerolog.Logger
	Downloader Downloader
	Events     chan Message
	Group      *errgroup.Group
}

type Block struct {
	Index    json.Number `json:"index"`
	Hash     []byte      `json:"hash"`
	Contents chan []byte `json:"-"`
}
type Message struct {
	Undo     []Block    `json:"undo"`
	Advance  Block      `json:"advance"`
	Finished chan error `json:"-"`
}

type UndoFunc func(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error
type AdvanceFunc func(ctx context.Context, tx ledger.Transaction, slot uint64, txIndex int) error

func (h *Syncer) SpawnSyncFunc(group *errgroup.Group, ctx context.Context, undoFunc UndoFunc, advanceFunc AdvanceFunc) {
	group.Go(func() (err error) {
		// For every event we receive
		for event := range h.Events {
			defer func() {
				if panicCause := recover(); panicCause != nil {
					h.Logger.Error().Any("panicCause", panicCause).Msg("panic while processing blocks, aborting")
					err = fmt.Errorf("panic while processing blocks, aborting: %v", panicCause)
					event.Finished <- err
				}
			}()
			// First apply each undo
			for _, undo := range event.Undo {
				// Wait for the contents of the block
				contents := <-undo.Contents
				// Decode it from CBOR; byte 0 is a cbor array, byte 1 is the block era, and bytes 2 onward are the block itself
				blockType := uint(contents[1])
				block, err := ledger.NewBlockFromCbor(blockType, contents[2:])
				if err != nil {
					h.Logger.Warn().Str("blockHash", hex.EncodeToString(undo.Hash)).Err(err).Msg("Error decoding block for undo")
					event.Finished <- err
					return err
				}

				// Iterate over the transactions *in reverse* (to undo them in the reverse of the order they were advanced)
				txs := block.Transactions()
				slices.Reverse(txs)
				for index, tx := range txs {
					// And invoke the undo logic
					if err := undoFunc(ctx, tx, block.SlotNumber(), index); err != nil {
						h.Logger.Warn().Str("blockHash", hex.EncodeToString(undo.Hash)).Err(err).Msg("Error executing undo logic for transaction")
						event.Finished <- err
						return err
					}
				}
			}

			// Now, wait for the contents of the block we're applying
			contents := <-event.Advance.Contents
			// Parse it
			blockType := uint(contents[1])
			block, err := ledger.NewBlockFromCbor(blockType, contents[2:])
			if err != nil {
				h.Logger.Warn().Str("blockHash", hex.EncodeToString(event.Advance.Hash)).Err(err).Msg("Error decoding block for advance")
				event.Finished <- err
				return err
			}
			// And apply each transaction in order
			for index, tx := range block.Transactions() {
				if err := advanceFunc(ctx, tx, block.SlotNumber(), index); err != nil {
					h.Logger.Warn().Str("blockHash", hex.EncodeToString(event.Advance.Hash)).Err(err).Msg("Error executing advance logic for transaction")
					event.Finished <- err
					return err
				}
			}
			event.Finished <- nil
		}
		return nil
	})
}

func (h *Syncer) HandleOne(data []byte) chan error {
	finished := make(chan error, 1)
	var message Message
	if err := json.Unmarshal(data, &message); err != nil {
		finished <- err
		return finished
	}
	for idx := range message.Undo {
		message.Undo[idx].Contents = make(chan []byte, 1)
	}
	message.Advance.Contents = make(chan []byte, 1)
	message.Finished = finished

	for _, undo := range message.Undo {
		h.Group.Go(func() error { return h.Downloader.DownloadBlock(undo.Hash, undo.Contents) })
	}
	h.Group.Go(func() error { return h.Downloader.DownloadBlock(message.Advance.Hash, message.Advance.Contents) })

	h.Events <- message
	return finished
}
