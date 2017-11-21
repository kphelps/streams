package actors_test

import (
	. "github.com/kphelps/indexing-service/actors"

	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streams", func() {

	Context("End to End", func() {
		It("Executes", func() {
			done := make(chan int)
			source := NewSource(func() int {
				time.Sleep(time.Millisecond)
				return 1
			})
			transformer := NewStream(func(input int) int {
				return input + 1
			})
			sink := NewSink(func(input int) {
				done <- input
			})

			stream := source.
				AttachStream(transformer).
				AttachSink(sink)
			stream.Open()

			result := <-done
			Expect(result).To(Equal(2))
			stream.Close()
		})
	})
})
