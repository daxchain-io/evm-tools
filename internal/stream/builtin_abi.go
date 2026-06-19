package stream

// Built-in event ABIs for the standard token interfaces, so that
// events = ["Transfer", "Approval"] resolves with no extra config (see
// docs/design.md, "Event identification and decoding"). These cover only the
// events; non-event ABI elements are irrelevant to the stream.
//
// Note ERC-20 and ERC-721 both declare Transfer(address,address,uint256) and
// Approval(address,address,uint256). They share the same signature/topic0, so
// resolving a bare "Transfer" against the union of built-ins is unambiguous; the
// indexed-ness differs (ERC-721 indexes tokenId) but the signature does not.

// erc20EventABI is the JSON ABI for the ERC-20 events.
const erc20EventABI = `[
  {"type":"event","name":"Transfer","anonymous":false,"inputs":[
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"value","type":"uint256","indexed":false}
  ]},
  {"type":"event","name":"Approval","anonymous":false,"inputs":[
    {"name":"owner","type":"address","indexed":true},
    {"name":"spender","type":"address","indexed":true},
    {"name":"value","type":"uint256","indexed":false}
  ]}
]`

// erc721EventABI is the JSON ABI for the ERC-721 events.
const erc721EventABI = `[
  {"type":"event","name":"Transfer","anonymous":false,"inputs":[
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"tokenId","type":"uint256","indexed":true}
  ]},
  {"type":"event","name":"Approval","anonymous":false,"inputs":[
    {"name":"owner","type":"address","indexed":true},
    {"name":"approved","type":"address","indexed":true},
    {"name":"tokenId","type":"uint256","indexed":true}
  ]},
  {"type":"event","name":"ApprovalForAll","anonymous":false,"inputs":[
    {"name":"owner","type":"address","indexed":true},
    {"name":"operator","type":"address","indexed":true},
    {"name":"approved","type":"bool","indexed":false}
  ]}
]`

// erc1155EventABI is the JSON ABI for the ERC-1155 events.
const erc1155EventABI = `[
  {"type":"event","name":"TransferSingle","anonymous":false,"inputs":[
    {"name":"operator","type":"address","indexed":true},
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"id","type":"uint256","indexed":false},
    {"name":"value","type":"uint256","indexed":false}
  ]},
  {"type":"event","name":"TransferBatch","anonymous":false,"inputs":[
    {"name":"operator","type":"address","indexed":true},
    {"name":"from","type":"address","indexed":true},
    {"name":"to","type":"address","indexed":true},
    {"name":"ids","type":"uint256[]","indexed":false},
    {"name":"values","type":"uint256[]","indexed":false}
  ]},
  {"type":"event","name":"ApprovalForAll","anonymous":false,"inputs":[
    {"name":"account","type":"address","indexed":true},
    {"name":"operator","type":"address","indexed":true},
    {"name":"approved","type":"bool","indexed":false}
  ]},
  {"type":"event","name":"URI","anonymous":false,"inputs":[
    {"name":"value","type":"string","indexed":false},
    {"name":"id","type":"uint256","indexed":true}
  ]}
]`

// builtinABIs is the ordered list of standard interface ABIs consulted when a
// contract supplies no explicit abi/signatures. Order matters only for
// reporting; identical signatures across interfaces resolve to the same topic0.
var builtinABIs = []string{erc20EventABI, erc721EventABI, erc1155EventABI}

// builtinEvents parses and concatenates the built-in interface events. It
// panics on a malformed built-in (a programming error, caught by tests).
func builtinEvents() []eventABI {
	var all []eventABI
	for _, raw := range builtinABIs {
		evs, err := parseABI([]byte(raw))
		if err != nil {
			panic("stream: malformed built-in ABI: " + err.Error())
		}
		all = append(all, evs...)
	}
	return all
}
