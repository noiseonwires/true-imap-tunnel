package imap

import (
	"bytes"
	"testing"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/protocol"
)

func TestBuildAndExtractFrameRoundTrip(t *testing.T) {
	original := protocol.Frame{
		Type:     protocol.MsgData,
		StreamID: 1234,
		SeqID:    42,
		Payload:  bytes.Repeat([]byte("hello, IMAP tunnel! \x00\x01\xff"), 100),
	}
	frameBytes := protocol.Encode(original)

	for _, fmtName := range []string{"attachment", "text"} {
		t.Run(fmtName, func(t *testing.T) {
			opts := MessageOptions{Format: fmtName, AttachmentFilename: "my-custom.bin"}
			msg := buildMessage(frameBytes, time.Unix(1700000000, 0), opts)
			if !bytes.Contains(msg, []byte("Subject: TIT\r\n")) {
				t.Errorf("missing marker subject")
			}
			switch fmtName {
			case "attachment":
				if !bytes.Contains(msg, []byte("Content-Transfer-Encoding: base64\r\n")) {
					t.Errorf("missing CTE header")
				}
				if !bytes.Contains(msg, []byte(`filename="my-custom.bin"`)) {
					t.Errorf("missing filename hint: %s", msg)
				}
				if !bytes.Contains(msg, []byte("application/octet-stream")) {
					t.Errorf("expected octet-stream content type")
				}
			case "text":
				if !bytes.Contains(msg, []byte("Content-Type: text/plain; charset=us-ascii\r\n")) {
					t.Errorf("missing text/plain content type: %s", msg)
				}
				if bytes.Contains(msg, []byte("Content-Disposition")) {
					t.Errorf("text mode should not set Content-Disposition")
				}
			}

			got, err := extractFrame(msg)
			if err != nil {
				t.Fatalf("extractFrame: %v", err)
			}
			if !bytes.Equal(got, frameBytes) {
				t.Fatalf("frame bytes mismatch: got %d bytes, want %d bytes", len(got), len(frameBytes))
			}

			f, err := protocol.Decode(got)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if f.Type != original.Type || f.StreamID != original.StreamID || f.SeqID != original.SeqID {
				t.Errorf("header mismatch: %+v vs %+v", f, original)
			}
			if !bytes.Equal(f.Payload, original.Payload) {
				t.Errorf("payload mismatch")
			}
		})
	}
}

func TestBuildMessageFilenameEscaped(t *testing.T) {
	msg := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		Format:             "attachment",
		AttachmentFilename: `weird "name" with \backslash.dat`,
	})
	// Embedded quotes and backslashes must be escaped in the MIME
	// quoted-string parameter, otherwise the header is malformed.
	want := `filename="weird \"name\" with \\backslash.dat"`
	if !bytes.Contains(msg, []byte(want)) {
		t.Errorf("filename not properly escaped in:\n%s", msg)
	}
}

func TestExtractFrameLF(t *testing.T) {
	// Some servers may normalise CRLF to LF — make sure we tolerate that.
	msg := "Subject: TIT\nContent-Transfer-Encoding: base64\n\n" +
		"aGVsbG8=" // base64("hello")
	got, err := extractFrame([]byte(msg))
	if err != nil {
		t.Fatalf("extractFrame: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", string(got), "hello")
	}
}

func TestExtractFrameMissingSeparator(t *testing.T) {
	if _, err := extractFrame([]byte("Subject: TIT only headers no body")); err == nil {
		t.Errorf("expected error for missing header/body separator")
	}
}

func TestExtractFrameBadBase64(t *testing.T) {
	msg := "Subject: TIT\r\n\r\n!!!not base64!!!"
	if _, err := extractFrame([]byte(msg)); err == nil {
		t.Errorf("expected base64 decode error")
	}
}
