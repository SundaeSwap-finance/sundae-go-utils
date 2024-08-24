package sundaecli

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type Metrics struct {
	service    Service
	cloudwatch cloudwatchiface.CloudWatchAPI
}

func NewMetrics(service Service, cloudwatch cloudwatchiface.CloudWatchAPI) Metrics {
	return Metrics{
		service,
		cloudwatch,
	}
}

type MetricName string

const (
	ResponseTimeMetric MetricName = "ResponseTime"
)

type DimensionName string

const (
	ServiceNameDimension    DimensionName = "Service"
	ServiceVersionDimension DimensionName = "Version"
	OperationNameDimension  DimensionName = "OperationName"
)

func defaultDimensions(service Service) map[DimensionName]string {
	return map[DimensionName]string{
		ServiceNameDimension:    service.Name,
		ServiceVersionDimension: service.Version,
	}
}

func mapToDimensions(ms ...map[DimensionName]string) []*cloudwatch.Dimension {
	var dimensions []*cloudwatch.Dimension
	for _, ds := range ms {
		for k, v := range ds {
			if v == "" {
				continue
			}
			dimensions = append(dimensions, &cloudwatch.Dimension{
				Name:  aws.String(string(k)),
				Value: aws.String(v),
			})
		}
	}
	return dimensions
}

func (m Metrics) Event(ctx context.Context, name MetricName, dimensions ...map[DimensionName]string) {
	awsDimensions := mapToDimensions(append(dimensions, defaultDimensions(m.service))...)
	m.cloudwatch.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("sundae-services"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(string(name)),
				Timestamp:  aws.Time(time.Now()),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(1),
				Dimensions: awsDimensions,
			},
		},
	})
}

func (m Metrics) Timing(ctx context.Context, name MetricName, start time.Time, dimensions ...map[DimensionName]string) {
	awsDimensions := mapToDimensions(append(dimensions, defaultDimensions(m.service))...)
	_, err := m.cloudwatch.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("sundae-services"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(string(name)),
				Timestamp:  aws.Time(time.Now()),
				Unit:       aws.String("Milliseconds"),
				Value:      aws.Float64(float64(time.Since(start).Milliseconds())),
				Dimensions: awsDimensions,
			},
		},
	})
	if err != nil {
		fmt.Printf("Warning: couldn't publish timing for %v: %+v\n", name, err)
	}
}

func (m Metrics) Gauge(ctx context.Context, name MetricName, value float64, dimensions ...map[DimensionName]string) {
	awsDimensions := mapToDimensions(append(dimensions, defaultDimensions(m.service))...)
	m.cloudwatch.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("sundae-services"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(string(name)),
				Timestamp:  aws.Time(time.Now()),
				Unit:       aws.String("None"),
				Value:      aws.Float64(value),
				Dimensions: awsDimensions,
			},
		},
	})
}
