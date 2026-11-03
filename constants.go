package tradingview

// Error contexts passed to OnErrorCallback. Some are no longer produced and
// are kept so that code comparing against them still compiles.
const (
	InitErrorContext                                = "Initializing the connection"
	ReadFirstMessageErrorContext                    = "Reading the first message after establishing the connection"
	DecodeFirstMessageErrorContext                  = "Decoding the first message after establishing the connection"
	FirstMessageWithoutSessionIdErrorContext        = "Does not have 'session_id' property"
	ConnectionSetupMessagesErrorContext             = "Sending connection setup messages"
	SendMessageErrorContext                         = "Sending a message"
	SendKeepAliveMessageErrorContext                = "Sending the keep alive message"
	GetPayloadLengthErrorContext                    = "Getting the payload length"
	DecodeMessageErrorContext                       = "Decoding the JSON message"
	DecodedMessageHasErrorPropertyErrorContext      = "JSON message has an error message"
	DecodedMessageDoesNotIncludePayloadErrorContext = "JSON message does not include the payload"
	PayloadCantBeParsedErrorContext                 = "JSON payload couldn't be parsed"
	FinalPayloadCantBeParsedErrorContext            = "The final JSON payload of the socket message couldn't be parsed"
	FinalPayloadHasMissingPropertiesErrorContext    = "The final JSON payload doesn't have the expected data"
	ReadMessageErrorContext                         = "Error while reading new messages through the socket connection"

	ExtractFirstMessagePayloadErrorContext = "Extracting payload from first message"
	SetReadDeadlineErrorContext            = "Setting read deadline on connection"
	ParsePacketErrorContext                = "Parsing WebSocket packet"
	ServerErrorMessageContext              = "Received server error message"
	DecodeQuoteMessageErrorContext         = "Decoding quote message payload"
	MaxReconnectErrorContext               = "Maximum reconnection attempts reached"
	PingErrorContext                       = "Sending ping request"
	ReadTimeoutErrorContext                = "Read operation timed out"
	ExtractPayloadErrorContext             = "Extracting payload from WebSocket message"
	PingTimeoutErrorContext                = "Ping operation timed out"
	WebSocketPingErrorContext              = "WebSocket ping frame sending failed"

	SymbolErrorContext           = "Symbol rejected by the server"
	RedirectErrorContext         = "Following server redirects"
	ListenerOverflowErrorContext = "Listener queue overflow"
)
