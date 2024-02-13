package publish

import (
	"encoding/json"
	"github.com/gofrs/uuid"
)

type DefaultMessageParser struct {
	DefaultMessageType    string
	DefaultMessageGroupID string
	DefaultContentType    string
}

type MessageParams struct {
	MessageType     string
	MessageGroupID  string
	DeduplicationID string
	ContentType     string
	Body            string
}

func NewDefaultMessageParser() *DefaultMessageParser {
	return &DefaultMessageParser{
		DefaultMessageType:    "-",
		DefaultMessageGroupID: "default",
		DefaultContentType:    "application/json",
	}
}

func (d DefaultMessageParser) Parse(message interface{}) (params MessageParams, err error) {
	raw, err := json.Marshal(message)
	if err != nil {
		return
	}

	// due to 5 minuted hardcoded time by AWS we do not really want deduplication
	// but need to provide id when wanting to use MessageGroupId, so just want a unique id
	deduplicationID, err := uuid.NewV4()
	if err != nil {
		return
	}

	params.MessageType = d.DefaultMessageType
	params.MessageGroupID = d.DefaultMessageGroupID
	params.DeduplicationID = deduplicationID.String()
	params.Body = string(raw)
	params.ContentType = d.DefaultContentType

	return
}
