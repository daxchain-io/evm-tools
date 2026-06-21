package record

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// FuzzReaderNext hardens the JSONL contract parser: Reader.Next must never panic
// on arbitrary input — every malformed line is a returned error, never a crash.
// The reader consumes untrusted producer output, so this is the
// most security-relevant surface in the suite. The seed corpus also runs under
// the normal `go test` as regression cases.
func FuzzReaderNext(f *testing.F) {
	f.Add([]byte(`{"schema_version":1,"type":"event","name":"x","data":{}}` + "\n"))
	f.Add([]byte("native_transfer\n"))
	f.Add([]byte(""))
	f.Add([]byte("\n\n\n"))
	f.Add([]byte("{}\n{}\n"))
	f.Add([]byte(`{"schema_version":999}` + "\n"))               // unsupported version
	f.Add([]byte(`{"schema_version":1,"type":"event"}` + "\n{")) // valid line then a torn line
	f.Add([]byte(`{"schema_version":1,"data":{"a":` + "\n"))     // truncated JSON
	f.Add([]byte("\x00\xff\xfe not utf8\n"))

	f.Fuzz(func(_ *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		// Bounded so a pathological input can't spin forever; Next returns io.EOF
		// (or another error) on real input long before this.
		for range 4096 {
			_, err := r.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				// Any other error is a legitimate "bad line" outcome — must be an
				// error, not a panic. Keep going: the reader may recover on the next
				// line, and we want to exercise that path too.
				continue
			}
		}
	})
}
