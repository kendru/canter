// Code generated by "stringer -type AssertMode -trimprefix AssertMode"; DO NOT EDIT.

package store

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[AssertModeInvalid-0]
	_ = x[AssertModeAddition-1]
	_ = x[AssertModeRetraction-2]
	_ = x[AssertModeRedaction-3]
}

const _AssertMode_name = "InvalidAdditionRetractionRedaction"

var _AssertMode_index = [...]uint8{0, 7, 15, 25, 34}

func (i AssertMode) String() string {
	if i >= AssertMode(len(_AssertMode_index)-1) {
		return "AssertMode(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _AssertMode_name[_AssertMode_index[i]:_AssertMode_index[i+1]]
}
