package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/google/tink/go/aead"
	"github.com/google/tink/go/insecurecleartextkeyset"
	"github.com/google/tink/go/keyset"
)

const (
//keySetString = "CMKIrNYJEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIGNoYW5nZSB0aGlzIHBhc3N3b3JkIHRvIGEgc2VjcmV0GAEQARjCiKzWCSABEmQKWAowdHlwZS5nb29nbGVhcGlzLmNvbS9nb29nbGUuY3J5cHRvLnRpbmsuQWVzR2NtS2V5EiIaIBSoByeI14YPzNqAZiuvDrDjOZ0cSoWF+OKpt69rAsaqGAEQARiZ8OimBCAB"
)

func main() {

	// decoded, err := base64.StdEncoding.DecodeString(keySetString)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// ksr := keyset.NewBinaryReader(bytes.NewBuffer(decoded))
	// ks, err := ksr.Read()
	// if err != nil {
	// 	log.Fatal(err)
	// }

	body, err := ioutil.ReadFile("keyset2.json")
	if err != nil {
		log.Fatalf("unable to read file: %v", err)
	}
	ksr := keyset.NewJSONReader(bytes.NewBuffer(body))
	ks, err := ksr.Read()
	if err != nil {
		log.Fatal(err)
	}

	handle, err := insecurecleartextkeyset.Read(&keyset.MemReaderWriter{Keyset: ks})
	if err != nil {
		log.Fatal(err)
	}

	a, err := aead.New(handle)
	if err != nil {
		log.Fatal(err)
	}

	buf := new(bytes.Buffer)
	w := keyset.NewJSONWriter(buf)
	if err := w.Write(ks); err != nil {
		log.Printf("Could not write encrypted keyhandle %v", err)
		return
	}
	var prettyJSON bytes.Buffer
	error := json.Indent(&prettyJSON, buf.Bytes(), "", "\t")
	if error != nil {
		log.Fatalf("JSON parse error: %v ", error)

	}
	log.Println("Tink Keyset (json) :\n", string(prettyJSON.Bytes()))

	bbw := new(bytes.Buffer)
	bw := keyset.NewBinaryWriter(bbw)
	if err := bw.Write(ks); err != nil {
		log.Fatalf("Could not write encrypted keyhandle %v", err)
	}

	log.Println("Tink Keyset Encoded: ", base64.StdEncoding.EncodeToString(bbw.Bytes()))

	// test encryption/decryption

	ec, err := a.Encrypt([]byte("foo"), []byte(""))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Encrypted Data: %s", base64.StdEncoding.EncodeToString(ec))

	// now read the whole thing back from scratch

	buf2 := bytes.NewBuffer(prettyJSON.Bytes())
	r := keyset.NewJSONReader(buf2)
	kh2, err := insecurecleartextkeyset.Read(r)
	if err != nil {
		log.Printf("Could not create TINK keyHandle %v", err)
		return
	}

	b, err := aead.New(kh2)
	if err != nil {
		log.Fatal(err)
	}

	dc, err := b.Decrypt(ec, []byte(""))
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Plain text: %s\n", string(dc))

}
