package cardano

import (
	"fmt"
	"time"
)

const (
	SlotOffsetPreview = 1666656000
	SlotOffsetMainnet = 1591566291
)

func SlotToTimeEnv(slot uint64, env string) (time.Time, error) {
	switch env {
	case "preview":
		return SlotToTime(slot, SlotOffsetPreview), nil
	case "mainnet":
		return SlotToTime(slot, SlotOffsetMainnet), nil
	default:
		return time.Time{}, fmt.Errorf("unrecognized environment %v", env)
	}
}

func SlotToTime(slot uint64, offset uint64) time.Time {
	return time.Unix(int64(slot+offset), 0)
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
