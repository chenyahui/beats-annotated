// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pipeline

import (
	"sync"

	"github.com/elastic/beats/libbeat/logp"
)

// retryer is responsible for accepting and managing failed send attempts. It
// will also accept not yet published events from outputs being dynamically closed
// by the controller. Cancelled batches will be forwarded to the new workQueue,
// without updating the events retry counters.
// If too many batches (number of outputs/3) are stored in the retry buffer,
// will the consumer be paused, until some batches have been processed by some
// outputs.
type retryer struct {
	logger   *logp.Logger
	observer outputObserver

	done chan struct{}

	consumer *eventConsumer

	sig        chan retryerSignal
	out        workQueue
	in         retryQueue
	doneWaiter sync.WaitGroup
}

type retryQueue chan batchEvent

type retryerSignal struct {
	tag     retryerEventTag
	channel workQueue
}

type batchEvent struct {
	tag   retryerBatchTag
	batch *Batch
}

type retryerEventTag uint8

const (
	sigRetryerOutputAdded retryerEventTag = iota
	sigRetryerOutputRemoved
	sigRetryerUpdateOutput
)

type retryerBatchTag uint8

const (
	retryBatch retryerBatchTag = iota
	cancelledBatch
)

func newRetryer(
	log *logp.Logger,
	observer outputObserver,
	out workQueue, // 初始化为nil
	c *eventConsumer,
) *retryer {
	r := &retryer{
		logger:     log,
		observer:   observer,
		done:       make(chan struct{}),
		sig:        make(chan retryerSignal, 3),
		in:         retryQueue(make(chan batchEvent, 3)),
		out:        out,
		consumer:   c,
		doneWaiter: sync.WaitGroup{},
	}
	r.doneWaiter.Add(1)
	go r.loop()
	return r
}

func (r *retryer) close() {
	close(r.done)
	//Block until loop() is properly closed
	r.doneWaiter.Wait()
}

func (r *retryer) sigOutputAdded() {
	r.sig <- retryerSignal{tag: sigRetryerOutputAdded}
}

func (r *retryer) sigOutputRemoved() {
	r.sig <- retryerSignal{tag: sigRetryerOutputRemoved}
}

// 这个workQueue就是在output.go中，被各个worker消费的queue
func (r *retryer) updOutput(ch workQueue) {
	r.sig <- retryerSignal{
		tag:     sigRetryerUpdateOutput,
		channel: ch,
	}
}

// output调用batch.Retry() 或者 batch.RetryEvents(events []Event)时，最终会转发到这里
func (r *retryer) retry(b *Batch) {
	r.in <- batchEvent{tag: retryBatch, batch: b}
}

func (r *retryer) cancelled(b *Batch) {
	r.in <- batchEvent{tag: cancelledBatch, batch: b}
}

func (r *retryer) loop() {
	defer r.doneWaiter.Done()
	var (
		out             workQueue
		consumerBlocked bool

		active     *Batch
		activeSize int
		buffer     []*Batch // 需要重试的batch
		numOutputs int

		log = r.logger
	)

	for {
		select {
		case <-r.done:
			return
		case evt := <-r.in: // retry
			var (
				countFailed  int
				countDropped int
				batch        = evt.batch
				countRetry   = len(batch.events)
			)

			if evt.tag == retryBatch {
				countFailed = len(batch.events)
				r.observer.eventsFailed(countFailed)

				// 去除掉TTL过期或者是不需要保证成功的event
				decBatch(batch)

				countRetry = len(batch.events)
				countDropped = countFailed - countRetry
				r.observer.eventsDropped(countDropped)
			}

			if len(batch.events) == 0 {
				log.Info("Drop batch")
				batch.Drop()
			} else {
				// 将out置为用户消费的queue
				out = r.out
				buffer = append(buffer, batch)
				out = r.out
				active = buffer[0]
				activeSize = len(active.events)
				if !consumerBlocked {
					consumerBlocked = blockConsumer(numOutputs, len(buffer))
					if consumerBlocked {
						log.Info("retryer: send wait signal to consumer")
						r.consumer.sigWait()
						log.Info("  done")
					}
				}
			}

		case out <- active: // 将active丢到outchannel中，供client重新消费
			r.observer.eventsRetry(activeSize)

			buffer = buffer[1:]
			active, activeSize = nil, 0

			if len(buffer) == 0 {
				out = nil
			} else {
				active = buffer[0]
				activeSize = len(active.events)
			}

			if consumerBlocked {
				consumerBlocked = blockConsumer(numOutputs, len(buffer))
				if !consumerBlocked {
					log.Info("retryer: send unwait-signal to consumer")
					r.consumer.sigUnWait()
					log.Info("  done")
				}
			}

		case sig := <-r.sig:
			switch sig.tag {
			case sigRetryerUpdateOutput:
				r.out = sig.channel
			case sigRetryerOutputAdded:
				numOutputs++
			case sigRetryerOutputRemoved:
				numOutputs--
			}
		}
	}
}

func blockConsumer(numOutputs, numBatches int) bool {
	return numBatches/3 >= numOutputs
}

// batch的ttl与output定制的output
func decBatch(batch *Batch) {
	if batch.ttl <= 0 {
		return
	}

	batch.ttl--
	if batch.ttl > 0 {
		return
	}

	// 如果ttl等于0，则进行过滤
	// 但是对于Guaranteed的event，似乎这个ttl也没啥作用
	// filter for evens with guaranteed send flags
	// event是否是Guaranteed，是由各自的beat定义的，使用方无法配置决定
	// 例如，filebeat就决定了event是Guaranteed的
	events := batch.events[:0]
	for _, event := range batch.events {
		if event.Guaranteed() {
			events = append(events, event)
		}
	}
	batch.events = events
}
