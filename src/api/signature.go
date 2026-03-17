package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
)

/*
Signature calculation scheme
1. Create canonical request:
canonical request example:
"<HTTPMethod>\n
<CanonicalURI>\n
<CanonicalHeaders>\n"
2. Concatenate canonical request string with user secret key
3. Use HMAC-sha256 to create signature
*/
func getCanonicalHeaders(r *http.Request) (string, error) {
	date := r.Header.Values("x-date")
	if len(date) != 1 {
		return "", ErrBadRequest
	}
	key := r.Header.Values("AccessKey")
	if len(key) != 1 {
		return "", ErrBadRequest
	}

	commonHeaders := fmt.Sprintf("x-date:%s\nAccessKey:%s", date[0], key[0])

	if r.Method == "PUT" {
		contentType := r.Header.Values("Content-Type")
		if len(contentType) != 1 {
			return "", ErrBadRequest
		}
		etag := r.Header.Values("Etag")
		if len(etag) != 1 {
			return "", ErrBadRequest
		}
		canonicalHeaders := fmt.Sprintf("%s\nContent-Type:%s\nEtag:%s",
			commonHeaders, contentType[0], etag[0])
		return canonicalHeaders, nil
	}

	return commonHeaders, nil
}

func SignReq(r *http.Request, secret string) (string, error) {
	canonicalHeaders, err := getCanonicalHeaders(r)
	if err != nil {
		return "", err
	}
	canonicalReq := fmt.Sprintf("%s\n%s\n%s\n", r.Method, r.URL.Path, canonicalHeaders)
	slog.Debug("Signing request", "canonical request", canonicalReq)
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(canonicalReq))
	signature := h.Sum(nil)
	return hex.EncodeToString(signature), nil
}
