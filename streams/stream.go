package actors

import (
	"fmt"
	"reflect"
)

type streamControlMessage struct {
	responseChannel chan<- error
}

func stopStreamMessage() (streamControlMessage, <-chan error) {
	channel := make(chan error)
	return streamControlMessage{
		responseChannel: channel,
	}, channel
}

type RunnableStream interface {
	Open()
	Close()
}

type runnableStream struct {
	source Source
	sink   Sink
}

func (rs *runnableStream) Open() {
	toSink := rs.source.open()
	rs.sink.open(toSink)
	rs.sink.start()
	rs.source.start()
}

func (rs *runnableStream) Close() {
	rs.source.close()
}

type BaseStream interface {
	start()
}

type Source interface {
	BaseStream
	open() chan interface{}
	close()
	AttachStream(Stream) Source
	AttachSink(Sink) RunnableStream
}

type BaseSourceMixin struct {
	signal chan streamControlMessage
	output chan interface{}
}

func (bsm *BaseSourceMixin) open() chan interface{} {
	bsm.signal = make(chan streamControlMessage)
	bsm.output = make(chan interface{}, 10)
	return bsm.output
}

func (bsm *BaseSourceMixin) close() {
	message, signal := stopStreamMessage()
	bsm.signal <- message
	<-signal
	close(bsm.output)
}

func ConnectSourceStream(source Source, stream Stream) Source {
	return &sourceStream{
		source: source,
		stream: stream,
	}
}

func ConnectSourceSink(source Source, sink Sink) RunnableStream {
	return &runnableStream{
		source: source,
		sink:   sink,
	}
}

func ConnectStreamSink(stream Stream, sink Sink) Sink {
	return &streamSink{
		stream: stream,
		sink:   sink,
	}
}

func ConnectStreamStream(left Stream, right Stream) Stream {
	return &streamStream{
		left:  left,
		right: right,
	}
}

type sourceStream struct {
	source Source
	stream Stream
}

func (ss *sourceStream) open() chan interface{} {
	toStream := ss.source.open()
	return ss.stream.open(toStream)
}

func (ss *sourceStream) close() {
	ss.source.close()
}

func (ss *sourceStream) start() {
	ss.stream.start()
	ss.source.start()
}

func (ss *sourceStream) AttachStream(stream Stream) Source {
	return ConnectSourceStream(ss, stream)
}

func (ss *sourceStream) AttachSink(sink Sink) RunnableStream {
	return ConnectSourceSink(ss, sink)
}

type Stream interface {
	BaseStream
	open(chan interface{}) chan interface{}
	AttachSource(Source) Source
	PrependStream(Stream) Stream
	AppendStream(Stream) Stream
	AttachSink(Sink) Sink
}

type BaseStreamMixin struct {
	input  chan interface{}
	output chan interface{}
}

func (ss *BaseStreamMixin) open(fromSource chan interface{}) chan interface{} {
	ss.input = fromSource
	ss.output = make(chan interface{}, 10)
	return ss.output
}

type streamStream struct {
	left  Stream
	right Stream
}

func (ss *streamStream) open(input chan interface{}) chan interface{} {
	leftOutput := ss.left.open(input)
	return ss.right.open(leftOutput)
}

func (ss *streamStream) start() {
	ss.right.start()
	ss.left.start()
}

func (ss *streamStream) AttachSource(source Source) Source {
	return ConnectSourceStream(source, ss)
}

func (ss *streamStream) AttachSink(sink Sink) Sink {
	return ConnectStreamSink(ss, sink)
}

func (ss *streamStream) PrependStream(stream Stream) Stream {
	return ConnectStreamStream(stream, ss)
}

func (ss *streamStream) AppendStream(stream Stream) Stream {
	return ConnectStreamStream(ss, stream)
}

type Sink interface {
	BaseStream
	open(chan interface{})
	AttachStream(Stream) Sink
	AttachSource(Source) RunnableStream
}

type BaseSinkMixin struct {
	input chan interface{}
}

func (ss *BaseSinkMixin) open(input chan interface{}) {
	ss.input = input
}

type streamSink struct {
	BaseSinkMixin
	stream Stream
	sink   Sink
}

func (ss *streamSink) open(input chan interface{}) {
	streamOutput := ss.stream.open(input)
	ss.sink.open(streamOutput)
}

func (ss *streamSink) start() {
	ss.sink.start()
	ss.stream.start()
}

func (ss *streamSink) AttachStream(stream Stream) Sink {
	return ConnectStreamSink(stream, ss)
}

func (ss *streamSink) AttachSource(source Source) RunnableStream {
	return ConnectSourceSink(source, ss)
}

type functionSource struct {
	BaseSourceMixin
	fn interface{}
}

func NewSource(fn interface{}) Source {
	return &functionSource{
		fn: fn,
	}
}

func (fs *functionSource) AttachStream(stream Stream) Source {
	return ConnectSourceStream(fs, stream)
}

func (fs *functionSource) AttachSink(sink Sink) RunnableStream {
	return ConnectSourceSink(fs, sink)
}

func (fs *functionSource) start() {
	go func() {
	loop:
		for {
			select {
			case message, ok := <-fs.signal:
				if !ok {
					break loop
				}
				message.responseChannel <- nil
				break loop
			default:
				value := dynamicCall(fs.fn)[0].Interface()
				fs.output <- value
			}
		}
	}()
}

type functionStream struct {
	BaseStreamMixin
	fn interface{}
}

func NewStream(fn interface{}) Stream {
	return &functionStream{
		fn: fn,
	}
}

func (fs *functionStream) AttachSource(source Source) Source {
	return ConnectSourceStream(source, fs)
}

func (fs *functionStream) AttachSink(sink Sink) Sink {
	return ConnectStreamSink(fs, sink)
}

func (fs *functionStream) PrependStream(stream Stream) Stream {
	return ConnectStreamStream(stream, fs)
}

func (fs *functionStream) AppendStream(stream Stream) Stream {
	return ConnectStreamStream(fs, stream)
}

func (fs *functionStream) start() {
	go func() {
	loop:
		for {
			select {
			case message, ok := <-fs.input:
				if !ok {
					break loop
				}
				value := dynamicCall(fs.fn, message)[0].Interface()
				fs.output <- value
			}
		}
	}()
}

type functionSink struct {
	BaseSinkMixin
	fn interface{}
}

func NewSink(fn interface{}) Sink {
	return &functionSink{
		fn: fn,
	}
}

func (fs *functionSink) AttachStream(stream Stream) Sink {
	return ConnectStreamSink(stream, fs)
}

func (fs *functionSink) AttachSource(source Source) RunnableStream {
	return ConnectSourceSink(source, fs)
}

func (fs *functionSink) start() {
	go func() {
	loop:
		for {
			select {
			case message, ok := <-fs.input:
				if !ok {
					break loop
				}
				dynamicCall(fs.fn, message)
			}
		}
	}()
}

func dynamicCall(fn interface{}, values ...interface{}) []reflect.Value {
	fnType := reflect.TypeOf(fn)
	if fnType.Kind() != reflect.Func {
		panic("Invalid stream function")
	}

	if fnType.NumIn() != len(values) {
		panic("Invalid function call, mismatch arg count")
	}

	for i := 0; i < fnType.NumIn(); i++ {
		fnArgType := fnType.In(i)
		valueType := reflect.TypeOf(values[i])
		if !valueType.ConvertibleTo(fnArgType) {
			panic(fmt.Sprintf("Attmpting to call %v with %v (arg %v)", fnType, values[i], i))
		}
	}

	dynamicValues := make([]reflect.Value, len(values))
	for i, value := range values {
		dynamicValues[i] = reflect.ValueOf(value)
	}
	fnValue := reflect.ValueOf(fn)
	return fnValue.Call(dynamicValues)
}
