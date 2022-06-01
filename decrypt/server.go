package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/google/tink/go/aead"
	"github.com/google/tink/go/insecurecleartextkeyset"
	"github.com/google/tink/go/keyset"
	"github.com/google/tink/go/tink"
	"golang.org/x/net/http2"
)

type bqRequest struct {
	RequestId          string            `json:"requestId"`
	Caller             string            `json:"caller"`
	SessionUser        string            `json:"sessionUser"`
	UserDefinedContext map[string]string `json:"userDefinedContext"`
	Calls              [][]interface{}   `json:"calls"`
}

type bqResponse struct {
	Replies      []string `json:"replies,omitempty"`
	ErrorMessage string   `json:"errorMessage,omitempty"`
}

const (
// static aead key
// key = "CMKIrNYJEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIGNoYW5nZSB0aGlzIHBhc3N3b3JkIHRvIGEgc2VjcmV0GAEQARjCiKzWCSABEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIBSoByeI14YPzNqAZiuvDrDjOZ0cSoWF+OKpt69rAsaqGAEQARiZ8OimBCAB"
)

var (
	a tink.AEAD
)

func init() {

	jk := os.Getenv("KEYSET")
	if jk == "" {
		panic(fmt.Errorf("KEYSET Environment variable must be set"))
	}

	ksr := keyset.NewJSONReader(bytes.NewBuffer([]byte(jk)))
	ks, err := ksr.Read()
	if err != nil {
		panic(fmt.Errorf("Error generating keyset reader %v", err))
	}

	nkh, err := insecurecleartextkeyset.Read(&keyset.MemReaderWriter{Keyset: ks})
	if err != nil {
		panic(fmt.Errorf("Error reading insecurecleartextkeyset %v", err))
	}

	a, err = aead.New(nkh)
	if err != nil {
		panic(fmt.Errorf("Error create AEAD %v", err))
	}

}

func AEAD_DECRYPT(w http.ResponseWriter, r *http.Request) {

	bqReq := &bqRequest{}
	bqResp := &bqResponse{}

	if err := json.NewDecoder(r.Body).Decode(&bqReq); err != nil {
		bqResp.ErrorMessage = fmt.Sprintf("External Function error: can't read POST body %v", err)
	} else {

		fmt.Printf("caller %s\n", bqReq.Caller)
		fmt.Printf("sessionUser %s\n", bqReq.SessionUser)
		fmt.Printf("userDefinedContext %v\n", bqReq.UserDefinedContext)

		wait := new(sync.WaitGroup)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		objs := make([]string, len(bqReq.Calls))

		for i, r := range bqReq.Calls {
			if len(r) != 2 {
				bqResp.ErrorMessage = fmt.Sprintf("Invalid number of input fields provided.  expected 2, got  %d", len(r))
			}

			raw, ok := r[0].(string)
			if !ok {
				bqResp.ErrorMessage = "Invalid cleartext type. expected string"
			}
			if bqResp.ErrorMessage != "" {
				bqResp.Replies = nil
				break
			}
			aad, ok := r[1].(string)
			if !ok {
				bqResp.ErrorMessage = "Invalid aad type. expected string"
			}
			if bqResp.ErrorMessage != "" {
				bqResp.Replies = nil
				break
			}

			//  use goroutines heres but keep the order
			wait.Add(1)
			go func(j int) {
				defer wait.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						b, err := base64.StdEncoding.DecodeString(raw)
						if err != nil {
							bqResp.ErrorMessage = fmt.Sprintf("Error b64decoding row %d", j)
							bqResp.Replies = nil
							cancel()
							return
						}
						ec, err := a.Decrypt(b, []byte(aad))
						if err != nil {
							bqResp.ErrorMessage = fmt.Sprintf("Error decrypting row %d", j)
							bqResp.Replies = nil
							cancel()
							return
						}
						objs[j] = string(ec)
						return
					}
				}
			}(i)
		}
		wait.Wait()
		if bqResp.ErrorMessage != "" {
			bqResp.Replies = nil
		} else {
			bqResp.Replies = objs
		}
	}

	b, err := json.Marshal(bqResp)
	if err != nil {
		http.Error(w, fmt.Sprintf("can't convert response to JSON %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func main() {

	http.HandleFunc("/", AEAD_DECRYPT)

	var server *http.Server
	server = &http.Server{
		Addr: ":8080",
	}
	http2.ConfigureServer(server, &http2.Server{})
	log.Println("Starting Server..")
	err := server.ListenAndServe()
	log.Fatalf("Unable to start Server %v", err)
}
