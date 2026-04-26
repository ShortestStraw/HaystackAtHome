package allocator

import (
	"HaystackAtHome/internal/ss/service/accumulator"
)

// allocator must use write and read streams accumulators to refer them when 
// determine which volume use to next write
// delete is not accumulated since it is hard to use it in allocation algorithms
type Allocator interface {
	// takes object size to be written and returns volume 
	// key for which write is the most cheapest among all volumes
	Next(uint64) uint64 
}

type accums struct {
	waccum     *accumulator.Accumulator
	raccum     *accumulator.Accumulator
}

// since we know that volumes are created with sequetially growing keys we can just traverse from 0 to vol_num
// in loop. The problem with this allocator is fast storage degradation. We are continiously combat for rwlock with growing
// num of readers so cpu usage is rising and at 0.5-1TiBs we are reaching the limits of sync package
type AllocatorRR struct {
	round uint64
	max   uint64
}

func NewRR(volNum uint64) *AllocatorRR {
	return &AllocatorRR{
		round: 0,
		max: volNum,
	}
}

func (a *AllocatorRR) Next(size uint64) uint64 {
	next := a.round
	a.round = (a.round + 1) % a.max
	return next
}

// Exponential generator is working with following rules: 
//  1) If volume was created less then N time ago and volume size is 30% of maximum use it for all writes
//  2) If volume raccum.Collect is twice higher for last 5 minutes than for last hour continue, otherwise use it for write
// for 1)-2) if volume space is expired then use next rule.
// Collect 5 min, 1 hr, 12 hr for every one
//  3) Choose the lowest read and the lowest write vol. If it the same one check if we have space the return it.
//  Otherwise calculate for each volume the following metric:
//
//  metric := w_1 * wr5min + w_2 * rd5min + w_3 * wr1hr + w_4 * rd1hr + w_5 * wr12hr + w_6 * rd12hr
//
//  sort volumes by them and choose one with the lowest metric
type AllocatorExp struct {}