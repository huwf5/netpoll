package netpoll

// ------------------------------------------ implement FDOperator ------------------------------------------
// TODO: implement read/write/hup function

func (f *Fifo) OnHup(p Poll) error {
	return nil
}

func (f *Fifo) Inputs(vs [][]byte) (rs [][]byte) {
	return nil
}

func (f *Fifo) InputAck(n int) (err error) {
	return nil
}

func (f *Fifo) Outputs(vs [][]byte) (rs [][]byte, supportZeroCopy bool) {
	return nil, false
}

func (f *Fifo) OutputAck(n int) (err error) {
	return nil
}
