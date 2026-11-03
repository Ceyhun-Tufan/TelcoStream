package domain

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrEmptyEventID      = errors.New("event_id cannot be empty")
	ErrEmptySubscriberID = errors.New("subscriber_id cannot be empty")
	ErrInvalidDataUsage  = errors.New("data_used_mb must be greater than zero")
	ErrEmptyCellTowerID  = errors.New("cell_tower_id cannot be empty")
)

type CDREvent struct {
	EventID      string    `json:"event_id"`
	SubscriberID string    `json:"subscriber_id"`
	DataUsedMB   int64     `json:"data_used_mb"`
	CellTowerID  string    `json:"cell_tower_id"`
	Timestamp    time.Time `json:"timestamp"`
}

// gelen cdr verisinin uygunlugunu kontrol etmeliyiz.
func (c *CDREvent) Validate() error {
	if strings.TrimSpace(c.EventID) == "" {
		return ErrEmptyEventID
	}
	if strings.TrimSpace(c.SubscriberID) == "" {
		return ErrEmptySubscriberID
	}
	if c.DataUsedMB <= 0 {
		return ErrInvalidDataUsage
	}
	if strings.TrimSpace(c.CellTowerID) == "" {
		return ErrEmptyCellTowerID
	}
	if c.Timestamp.IsZero() {
		c.Timestamp = time.Now().UTC()
	}
	return nil
}
