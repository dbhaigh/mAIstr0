package main

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/maistr0/maistr0/internal/selfupdate"
)

func runSignRelease(args []string, out, errOut io.Writer) error {
	flags := flag.NewFlagSet("sign-release", flag.ContinueOnError)
	flags.SetOutput(errOut)
	manifestPath := flags.String("manifest", "", "path to a generated .peer.json manifest")
	privateKeyPath := flags.String("key", "", "path to an unencrypted PKCS#8 Ed25519 PEM private key")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *manifestPath == "" || *privateKeyPath == "" {
		return errors.New("usage: maistr0 sign-release --manifest <file.peer.json> --key <private-key.pem>")
	}
	publicKey, err := selfupdate.SignManifestFile(*manifestPath, *privateKeyPath)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "peer_update_publisher_key: %s\n", base64.StdEncoding.EncodeToString(publicKey))
	return err
}
