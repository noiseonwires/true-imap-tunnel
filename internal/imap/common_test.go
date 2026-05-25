package imap

import (
	"bytes"
	"testing"
	"time"

	"github.com/true-imap-tunnel/true-imap-tunnel/internal/config"
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
			msg, err := buildMessage(frameBytes, time.Unix(1700000000, 0), opts)
			if err != nil {
				t.Fatalf("buildMessage: %v", err)
			}
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
	msg, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		Format:             "attachment",
		AttachmentFilename: `weird "name" with \backslash.dat`,
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	// Embedded quotes and backslashes must be escaped in the MIME
	// quoted-string parameter, otherwise the header is malformed.
	want := `filename="weird \"name\" with \\backslash.dat"`
	if !bytes.Contains(msg, []byte(want)) {
		t.Errorf("filename not properly escaped in:\n%s", msg)
	}
}

func TestBuildMessageCustomSubject(t *testing.T) {
	msg, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		Subject: "hello tunnel",
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if !bytes.Contains(msg, []byte("Subject: hello tunnel\r\n")) {
		t.Fatalf("custom subject missing:\n%s", msg)
	}
}

func TestBuildMessageRandomSubject(t *testing.T) {
	oldReadRandom := readRandom
	defer func() { readRandom = oldReadRandom }()
	readRandom = func(b []byte) (int, error) {
		for i := range b {
			b[i] = byte(i)
		}
		return len(b), nil
	}

	msg, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		SubjectMode: config.MessageSubjectModeRandom,
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if !bytes.Contains(msg, []byte("Subject: 000102030405060708090a0b0c0d0e0f\r\n")) {
		t.Fatalf("random subject missing:\n%s", msg)
	}
}

func TestBuildMessageSubjectClientID(t *testing.T) {
	msg, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		Subject:         "hello tunnel",
		ClientID:        7,
		SubjectClientID: true,
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if !bytes.Contains(msg, []byte("Subject: 07 hello tunnel\r\n")) {
		t.Fatalf("client-id subject missing:\n%s", msg)
	}
}

func TestBuildMessageRandomSubjectClientID(t *testing.T) {
	oldReadRandom := readRandom
	defer func() { readRandom = oldReadRandom }()
	readRandom = func(b []byte) (int, error) {
		for i := range b {
			b[i] = byte(i)
		}
		return len(b), nil
	}

	msg, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		SubjectMode:     config.MessageSubjectModeRandom,
		ClientID:        7,
		SubjectClientID: true,
	})
	if err != nil {
		t.Fatalf("buildMessage: %v", err)
	}
	if !bytes.Contains(msg, []byte("Subject: 07 000102030405060708090a0b0c0d0e0f\r\n")) {
		t.Fatalf("random client-id subject missing:\n%s", msg)
	}
}

func TestSubjectClientIDParsing(t *testing.T) {
	tests := []struct {
		subject string
		want    byte
		wantOK  bool
	}{
		{subject: "07 hello tunnel", want: 7, wantOK: true},
		{subject: "ff random", want: 255, wantOK: true},
		{subject: "TIT", wantOK: false},
		{subject: "Re: 07 hello tunnel", wantOK: false},
		{subject: "zz hello tunnel", wantOK: false},
		{subject: "0700000000000000 hello tunnel", wantOK: false},
		{subject: "00 hello tunnel", wantOK: false},
	}
	for _, tc := range tests {
		got, ok := subjectClientID(tc.subject)
		if got != tc.want || ok != tc.wantOK {
			t.Fatalf("subjectClientID(%q) = %d/%v, want %d/%v", tc.subject, got, ok, tc.want, tc.wantOK)
		}
	}
}

func TestBuildMessageRejectsHeaderInjectionSubject(t *testing.T) {
	if _, err := buildMessage([]byte("x"), time.Unix(0, 0), MessageOptions{
		Subject: "safe\r\nInjected: yes",
	}); err == nil {
		t.Fatal("buildMessage accepted subject containing CRLF")
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
