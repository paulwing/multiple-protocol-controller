package modbusRtu

type FunCodeType struct {
	ReadCoil           int
	ReadDisperse       int
	ReadHoldRegister   int
	ReadInputRegister  int
	WriteCoil          int
	WriteHoldRegister  int
	WriteMultiCoil     int
	WriteMultiRegister int
}

var ModbusRtuFunCode = FunCodeType{
	ReadCoil:           1,  // ReadCoil
	ReadDisperse:       2,  // ReadDisperse
	ReadHoldRegister:   3,  // ReadHoldRegister
	ReadInputRegister:  4,  // ReadInputRegister
	WriteCoil:          5,  // WriteCoil
	WriteHoldRegister:  6,  // WriteHoldRegister
	WriteMultiCoil:     15, // WriteMultiCoil
	WriteMultiRegister: 16, // WriteMultiRegister
}
