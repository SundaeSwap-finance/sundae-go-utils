package cardano

import (
	"fmt"
	"time"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
)

type DateTime struct {
	Instant time.Time
}

func (d DateTime) UnixInt() int64 {
	return d.Instant.Unix()
}

func (d DateTime) Unix() string {
	return fmt.Sprintf("%d", d.Instant.Unix())
}

func (d DateTime) UnixMilliInt() int64 {
	return d.Instant.UnixMilli()
}

func (d DateTime) UnixMilli() string {
	return fmt.Sprintf("%d", d.Instant.UnixMilli())
}

func (d DateTime) Slot() (int32, error) {
	s, err := TimeToSlotEnv(d.Instant, "")
	return int32(s), err
}

const DefaultLayout = "2006-01-02T15:04:05Z"

func (d DateTime) Format(args struct{ Layout string }) string {
	return d.Instant.Format(args.Layout)
}

const (
	// ┌─────────┬────────────┬───────────────┬───────────────────────────────────────────────────────────────────────┐
	// │ Network │ Byron era  │  systemStart  │                          Offset calculation                           │
	// ├─────────┼────────────┼───────────────┼───────────────────────────────────────────────────────────────────────┤
	// │ Preview │ None       │ 1 666 656 000 │ offset = systemStart (slot 0 = start) = 1 666 656 000                 │
	// ├─────────┼────────────┼───────────────┼───────────────────────────────────────────────────────────────────────┤
	// │ Preprod │ 4 epochs   │ 1 654 041 600 │ offset = 1 654 041 600 + (86 400 × 20) - 86 400 = 1 655 683 200       │
	// ├─────────┼────────────┼───────────────┼───────────────────────────────────────────────────────────────────────┤
	// │ Mainnet │ 208 epochs │ 1 506 203 091 │ offset = 1 506 203 091 + (4 492 800 × 20) - 4 492 800 = 1 591 566 291 │
	// └─────────┴────────────┴───────────────┴───────────────────────────────────────────────────────────────────────┘
	SlotOffsetPreview = 1_666_656_000
	SlotOffsetPreprod = 1_655_683_200
	SlotOffsetMainnet = 1_591_566_291
)

func NetworkToSlotOffset(network string) (uint64, error) {
	if network == "" {
		network = sundaecli.CommonOpts.Network
	}
	switch network {
	case "preview":
		return SlotOffsetPreview, nil
	case "preprod":
		return SlotOffsetPreprod, nil
	case "mainnet", "cardano-tom":
		return SlotOffsetMainnet, nil
	default:
		if sundaecli.CommonOpts.SlotOffset != 0 {
			return sundaecli.CommonOpts.SlotOffset, nil
		} else {
			return 0, fmt.Errorf("unrecognized network %v", network)
		}
	}
}

// EnvToSlotOffset is deprecated, use NetworkToSlotOffset instead
func EnvToSlotOffset(env string) (uint64, error) {
	return NetworkToSlotOffset(env)
}

func SlotToTimeEnv(slot uint64, env string) (time.Time, error) {
	slotOffset, err := EnvToSlotOffset(env)
	if err != nil {
		return time.Time{}, err
	}
	return SlotToTime(slot, slotOffset), nil
}

func SlotToTime(slot uint64, offset uint64) time.Time {
	return time.Unix(int64(slot+offset), 0)
}

func SlotToDateTime(slot uint64, offset uint64) DateTime {
	return DateTime{SlotToTime(slot, offset)}
}

func SlotToDateTimeEnv(slot uint64, env string) (DateTime, error) {
	slotOffset, err := EnvToSlotOffset(env)
	if err != nil {
		return DateTime{}, err
	}
	return SlotToDateTime(slot, slotOffset), nil
}

func TimeToSlotNetwork(t time.Time, network string) (uint64, error) {
	slotOffset, err := NetworkToSlotOffset(network)
	if err != nil {
		return 0, err
	}
	return TimeToSlot(t, slotOffset), nil
}

// TimeToSlotEnv is deprecated, use TimeToSlotNetwork instead
func TimeToSlotEnv(t time.Time, env string) (uint64, error) {
	return TimeToSlotNetwork(t, env)
}

func TimeToSlot(time time.Time, offset uint64) uint64 {
	return uint64(time.Unix() - int64(offset))
}
