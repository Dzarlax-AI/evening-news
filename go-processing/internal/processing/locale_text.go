package processing

import "unicode"

// serbianLatinProse rejects a response that is predominantly Cyrillic while
// allowing source quotations and names to retain their original spelling.
func serbianLatinProse(text string) bool {
	latin, cyrillic := 0, 0
	var closing rune
	for _, r := range text {
		if closing != 0 {
			if r == closing {
				closing = 0
			}
			continue
		}
		switch r {
		case '«':
			closing = '»'
			continue
		case '„':
			closing = '“'
			continue
		case '“':
			closing = '”'
			continue
		case '"':
			closing = '"'
			continue
		}
		if unicode.In(r, unicode.Latin) {
			latin++
		}
		if unicode.In(r, unicode.Cyrillic) {
			cyrillic++
		}
	}
	return latin > 0 && latin > cyrillic*2
}
