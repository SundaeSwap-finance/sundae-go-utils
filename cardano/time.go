package cardano

import (
	"fmt"
	"time"

	sundaecli "github.com/SundaeSwap-finance/sundae-go-utils/sundae-cli"
)

type DateTime struct {
	Instant time.Time
}

func (d DateTime) Unix() int64 {
	return d.Instant.Unix()
}

func (d DateTime) Slot() (int32, error) {
	s, err := TimeToSlotEnv(d.Instant, "")
	return int32(s), err
}

func (d DateTime) Format(args struct{ Layout string }) string {
	return d.Instant.Format(args.Layout)
}

const (
	SlotOffsetPreview = 1666656000
	SlotOffsetMainnet = 1591566291
)

func EnvToSlotOffset(env string) (uint64, error) {
	switch env {
	case "preview":
		return SlotOffsetPreview, nil
	case "mainnet":
		return SlotOffsetMainnet, nil
	default:
		if sundaecli.CommonOpts.SlotOffset != 0 {
			return sundaecli.CommonOpts.SlotOffset, nil
		} else {
			return 0, fmt.Errorf("unrecognized environment %v", env)
		}
	}
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

func TimeToSlotEnv(time time.Time, env string) (uint64, error) {
	switch env {
	case "preview":
		return TimeToSlot(time, SlotOffsetPreview), nil
	case "mainnet":
		return TimeToSlot(time, SlotOffsetMainnet), nil
	default:
		return 0, fmt.Errorf("unrecognized environment %v", env)
	}
}

func TimeToSlot(time time.Time, offset uint64) uint64 {
	return uint64(time.Unix() - int64(offset))
}
