package replay

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// S3BlockSource streams archived blocks straight from an S3 bucket — an
// alternative to mounting the bucket on the local filesystem and pointing
// Config.BlockDir at the mount point.
//
// Use this when:
//   - running short-range replays locally without FUSE-mounting the bucket
//   - debugging a single Lambda binary against arbitrary preview / preprod /
//     mainnet history (the consumer framework wires this up automatically when
//     --bucket / --start-height are passed)
//
// Production large-scale migrations should keep using BlockDir + a mounted
// bucket: the kernel page cache there beats per-object S3 GETs by an order
// of magnitude.
type S3BlockSource struct {
	S3     s3iface.S3API
	Bucket string // bare bucket name or `s3://name[/prefix]`
}

func (s *S3BlockSource) FetchBlock(ctx context.Context, location, hashHex string) ([]byte, error) {
	bucket := strings.TrimPrefix(s.Bucket, "s3://")
	if i := strings.Index(bucket, "/"); i >= 0 {
		// Accept `s3://bucket/prefix` and prepend the prefix to the key. This
		// is rare for sync-v2 (the standard layout puts blocks at the root)
		// but harmless to support.
		location = strings.TrimSuffix(bucket[i:], "/") + "/" + location
		bucket = bucket[:i]
	}
	obj, err := s.S3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(location),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get s3://%s/%s (block %s): %w", bucket, location, hashHex, err)
	}
	defer obj.Body.Close()
	contents, err := io.ReadAll(obj.Body)
	if err != nil {
		return nil, fmt.Errorf("read s3://%s/%s (block %s): %w", bucket, location, hashHex, err)
	}
	return contents, nil
}
