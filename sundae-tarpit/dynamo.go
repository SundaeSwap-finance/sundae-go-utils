package tarpit

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Bait rows share one DynamoDB table with whatever else a service keeps there.
// They use a "bait:" hash-key prefix and "bait"-prefixed attribute names, so
// they stay out of any table's other items and GSIs. This matches the ban and
// intent conventions in rewards-distribution. The hash-key attribute is "PK",
// a string; a backing table must use that key for these helpers to work.
//
// A bait row is global: it is not scoped per client, because the bait applies
// to a caller across every route and every service that reads the shared table.
// To move bait into its own table later, create a table with a string "PK"
// hash key and point NewDynamoSource and the ban-claimer writer at it; nothing
// else changes.
//
// The request path reads one row by key (GetItem), never a Scan. The table can
// be large, so a per-request Scan would read the whole table and throttle its
// other users. Results are cached per IP, so a repeat caller does no read.
// Scan is used only by ListBait, for an operator listing off the request path.

const (
	baitPKPrefix      = "bait:"
	attrPK            = "PK"
	attrBaitReason    = "baitReason"
	attrBaitSource    = "baitSource"
	attrBaitUntil     = "baitUntil"     // unix seconds; 0 means permanent
	attrBaitCreatedAt = "baitCreatedAt" // unix seconds

	defaultCacheTTL = time.Minute
	cacheSizeCap    = 50000
)

// BaitPK returns the hash-key value for a bait row.
func BaitPK(ip string) string {
	return baitPKPrefix + strings.TrimSpace(ip)
}

// BaitRow is one flagged IP in the shared table.
type BaitRow struct {
	IP        string
	Reason    string
	Source    string
	Until     int64 // unix seconds; 0 means permanent
	CreatedAt int64 // unix seconds
}

// Active reports whether the row is in force at now.
func (r BaitRow) Active(now time.Time) bool {
	return r.Until == 0 || now.Unix() < r.Until
}

type cacheEntry struct {
	bait    bool
	expires time.Time
}

// DynamoSource answers IsBait from a DynamoDB table with one keyed GetItem per
// IP, cached for a TTL. A flagged IP is cached as bait and a normal IP as not
// bait, so steady-state traffic does about one GetItem per distinct IP per TTL.
// A newly added or removed bait IP takes effect within one TTL.
type DynamoSource struct {
	api   dynamodbiface.DynamoDBAPI
	table string
	ttl   time.Duration

	mu    sync.Mutex
	cache map[string]cacheEntry
}

// NewDynamoSource returns a DynamoSource for the given table. A ttl of zero
// uses a one-minute cache.
func NewDynamoSource(api dynamodbiface.DynamoDBAPI, table string, ttl time.Duration) *DynamoSource {
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	return &DynamoSource{api: api, table: table, ttl: ttl, cache: map[string]cacheEntry{}}
}

// IsBait reports whether ip has an active bait row, reading one row by key on a
// cache miss.
func (d *DynamoSource) IsBait(ctx context.Context, ip string) (bool, error) {
	if hit, ok := d.cached(ip); ok {
		return hit, nil
	}

	row, found, err := getBait(ctx, d.api, d.table, ip)
	if err != nil {
		return false, err
	}
	bait := found && row.Active(time.Now())
	d.store(ip, bait)
	return bait, nil
}

func (d *DynamoSource) cached(ip string) (bool, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	e, ok := d.cache[ip]
	if !ok || time.Now().After(e.expires) {
		return false, false
	}
	return e.bait, true
}

func (d *DynamoSource) store(ip string, bait bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Bound memory: when the cache grows past the cap, drop expired entries,
	// and if it is still at the cap, reset it. Entries are cheap to refill.
	if len(d.cache) >= cacheSizeCap {
		now := time.Now()
		for k, e := range d.cache {
			if now.After(e.expires) {
				delete(d.cache, k)
			}
		}
		if len(d.cache) >= cacheSizeCap {
			d.cache = make(map[string]cacheEntry, cacheSizeCap)
		}
	}
	d.cache[ip] = cacheEntry{bait: bait, expires: time.Now().Add(d.ttl)}
}

// getBait reads one bait row by key. An eventually-consistent read is enough;
// the TTL cache already tolerates slightly stale membership.
func getBait(ctx context.Context, api dynamodbiface.DynamoDBAPI, table, ip string) (BaitRow, bool, error) {
	out, err := api.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			attrPK: {S: aws.String(BaitPK(ip))},
		},
	})
	if err != nil {
		return BaitRow{}, false, fmt.Errorf("unable to read bait row for %v: %w", ip, err)
	}
	if len(out.Item) == 0 {
		return BaitRow{}, false, nil
	}
	return rowFromItem(out.Item), true, nil
}

// ipFromPK recovers the IP from a bait row's hash key.
func ipFromPK(pk string) string {
	return strings.TrimPrefix(pk, baitPKPrefix)
}

func rowFromItem(item map[string]*dynamodb.AttributeValue) BaitRow {
	row := BaitRow{}
	if v := item[attrPK]; v != nil && v.S != nil {
		row.IP = ipFromPK(*v.S)
	}
	if v := item[attrBaitReason]; v != nil && v.S != nil {
		row.Reason = *v.S
	}
	if v := item[attrBaitSource]; v != nil && v.S != nil {
		row.Source = *v.S
	}
	if v := item[attrBaitUntil]; v != nil && v.N != nil {
		row.Until = parseInt(*v.N)
	}
	if v := item[attrBaitCreatedAt]; v != nil && v.N != nil {
		row.CreatedAt = parseInt(*v.N)
	}
	return row
}

func parseInt(s string) int64 {
	var n int64
	_, _ = fmt.Sscan(s, &n)
	return n
}

// PutBait writes or replaces a bait row. A management tool such as ban-claimer
// calls this so it does not need the row schema. until is a unix-seconds
// expiry; 0 is permanent.
func PutBait(ctx context.Context, api dynamodbiface.DynamoDBAPI, table string, row BaitRow) error {
	if row.CreatedAt == 0 {
		row.CreatedAt = time.Now().Unix()
	}
	item := map[string]*dynamodb.AttributeValue{
		attrPK:            {S: aws.String(BaitPK(row.IP))},
		attrBaitCreatedAt: {N: aws.String(fmt.Sprintf("%d", row.CreatedAt))},
		attrBaitUntil:     {N: aws.String(fmt.Sprintf("%d", row.Until))},
	}
	if row.Reason != "" {
		item[attrBaitReason] = &dynamodb.AttributeValue{S: aws.String(row.Reason)}
	}
	if row.Source != "" {
		item[attrBaitSource] = &dynamodb.AttributeValue{S: aws.String(row.Source)}
	}
	_, err := api.PutItemWithContext(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("unable to write bait row for %v: %w", row.IP, err)
	}
	return nil
}

// ListBait returns every bait row in the table, active or expired. It scans the
// table, so it is for an operator listing, not the request path. A caller
// checks Active to tell active rows from expired ones.
func ListBait(ctx context.Context, api dynamodbiface.DynamoDBAPI, table string) ([]BaitRow, error) {
	var rows []BaitRow
	input := &dynamodb.ScanInput{
		TableName:        aws.String(table),
		FilterExpression: aws.String("begins_with(#pk, :prefix)"),
		ExpressionAttributeNames: map[string]*string{
			"#pk": aws.String(attrPK),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":prefix": {S: aws.String(baitPKPrefix)},
		},
	}
	err := api.ScanPagesWithContext(ctx, input, func(page *dynamodb.ScanOutput, _ bool) bool {
		for _, item := range page.Items {
			rows = append(rows, rowFromItem(item))
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("unable to scan bait rows from %v: %w", table, err)
	}
	return rows, nil
}

// DeleteBait removes the bait row for ip, if any.
func DeleteBait(ctx context.Context, api dynamodbiface.DynamoDBAPI, table, ip string) error {
	_, err := api.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(table),
		Key: map[string]*dynamodb.AttributeValue{
			attrPK: {S: aws.String(BaitPK(ip))},
		},
	})
	if err != nil {
		return fmt.Errorf("unable to delete bait row for %v: %w", ip, err)
	}
	return nil
}
