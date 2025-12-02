package net

import "fmt"

type PxcErrorType int
type CtrlErrorType int
type StrmErrorType int

const (
	PxcUnknownErr PxcErrorType = iota
	PxcDecodeErr
	PxcWriteErr
	PxcHudCfgErr
)

const (
	CtrlDecodeErr CtrlErrorType = iota + 1
	CtrlWriteErr
)

const (
	StrmUnknownCommandErr StrmErrorType = iota + 1
	StrmDecodeErr
	StrmWriteErr
)

type FatalError interface {
	error
	IsFatal() bool
}

type PxcError struct {
	Type     PxcErrorType
	SrcError error
	Fatal    bool
}

func (e *PxcError) Error() string {
	return fmt.Sprintf("%d (%s)", e.Type, e.SrcError.Error())
}

func (e *PxcError) IsFatal() bool { return e.Fatal }

type CtrlError struct {
	Type     CtrlErrorType
	SrcError error
	Fatal    bool
}

func (e *CtrlError) Error() string {
	return fmt.Sprintf("%d (%s)", e.Type, e.SrcError.Error())
}

func (e *CtrlError) IsFatal() bool { return e.Fatal }

type StrmError struct {
	Type     StrmErrorType
	SrcError error
	Fatal    bool
}

func (e *StrmError) Error() string {
	return fmt.Sprintf("%d (%s)", e.Type, e.SrcError.Error())
}

func (e *StrmError) IsFatal() bool { return e.Fatal }
