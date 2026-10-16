package nexus

// CallerNamespaceHeaderName is the Nexus header that names the caller namespace to a handler.
// Only the server sets it. The server refuses or removes a value that a client sends, so a
// handler can trust it.
const CallerNamespaceHeaderName = "temporal-caller-namespace"
