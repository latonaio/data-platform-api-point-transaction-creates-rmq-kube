package dpfm_api_output_formatter

import (
	dpfm_api_input_reader "data-platform-api-point-transaction-creates-rmq-kube/DPFM_API_Input_Reader"
	//dpfm_api_processing_formatter "data-platform-api-point-transaction-creates-rmq-kube/DPFM_API_Processing_Formatter"
	"data-platform-api-point-transaction-creates-rmq-kube/sub_func_complementer"
	"encoding/json"

	"golang.org/x/xerrors"
)

func ConvertToHeaderCreates(subfuncSDC *sub_func_complementer.SDC) (*Header, error) {
	data := subfuncSDC.Message.Header

	header, err := TypeConverter[*Header](data)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func ConvertToHeaderUpdates(headerData dpfm_api_input_reader.Header) (*Header, error) {
	data := headerData

	header, err := TypeConverter[*Header](data)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func ConvertToHeader(
	input *dpfm_api_input_reader.SDC,
	subfuncSDC *sub_func_complementer.SDC,
) *sub_func_complementer.SDC {
	subfuncSDC.Message.Header = &sub_func_complementer.Header{
		PointTransaction:                      *input.Header.PointTransaction,
		PointTransactionType:                  input.Header.PointTransactionType,
		PointTransactionDate:                  input.Header.PointTransactionDate,
		PointTransactionTime:                  input.Header.PointTransactionTime,
		Sender:                                input.Header.Sender,
		Receiver:                              input.Header.Receiver,
		PointSymbol:                           input.Header.PointSymbol,
		PlusMinus:                             input.Header.PlusMinus,
		PointTransactionAmount:                input.Header.PointTransactionAmount,
		PointTransactionObjectType:            input.Header.PointTransactionObjectType,
		PointTransactionObject:                input.Header.PointTransactionObject,
		SenderPointBalanceBeforeTransaction:   input.Header.SenderPointBalanceBeforeTransaction,
		SenderPointBalanceAfterTransaction:    input.Header.SenderPointBalanceAfterTransaction,
		ReceiverPointBalanceBeforeTransaction: input.Header.ReceiverPointBalanceBeforeTransaction,
		ReceiverPointBalanceAfterTransaction:  input.Header.ReceiverPointBalanceAfterTransaction,
		Attendance:							   input.Header.Attendance,
		Participation:						   input.Header.Participation,
		CreationDate:                          input.Header.CreationDate,
		CreationTime:                          input.Header.CreationTime,
		IsCancelled:                           input.Header.IsCancelled,
	}

	return subfuncSDC
}

func TypeConverter[T any](data interface{}) (T, error) {
	var dist T
	b, err := json.Marshal(data)
	if err != nil {
		return dist, xerrors.Errorf("Marshal error: %w", err)
	}
	err = json.Unmarshal(b, &dist)
	if err != nil {
		return dist, xerrors.Errorf("Unmarshal error: %w", err)
	}
	return dist, nil
}
