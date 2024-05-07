package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-point-transaction-creates-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-point-transaction-creates-rmq-kube/DPFM_API_Output_Formatter"
	dpfm_api_processing_formatter "data-platform-api-point-transaction-creates-rmq-kube/DPFM_API_Processing_Formatter"
	"data-platform-api-point-transaction-creates-rmq-kube/sub_func_complementer"
	"fmt"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
	"golang.org/x/xerrors"
)

func (c *DPFMAPICaller) createSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	subfuncSDC *sub_func_complementer.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var header *dpfm_api_output_formatter.Header

	//subfuncSDC.Message.Header = input.Header

	for _, fn := range accepter {
		switch fn {
		case "Header":
			var calculatePointTransactionQueryGets *sub_func_complementer.CalculatePointTransactionQueryGets
			var pointTransactionIssuedID int

			calculatePointTransactionQueryGets = c.CalculatePointTransaction(errs)

			if calculatePointTransactionQueryGets == nil {
				err := xerrors.Errorf("calculatePointTransactionQueryGets is nil")
				*errs = append(*errs, err)
				return nil
			}

			pointTransactionIssuedID = calculatePointTransactionQueryGets.PointTransactionLatestNumber + 1

			input.Header.PointTransaction = &pointTransactionIssuedID

			header = c.headerCreateSql(nil, mtx, input, output, subfuncSDC, errs, log)

			if calculatePointTransactionQueryGets != nil {
				err := c.UpdateLatestNumber(errs, pointTransactionIssuedID)
				if err != nil {
					*errs = append(*errs, err)
					return nil
				}
			}
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		Header: header,
	}

	return data
}

func (c *DPFMAPICaller) updateSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var header *dpfm_api_output_formatter.Header
	for _, fn := range accepter {
		switch fn {
		case "Header":
			header = c.headerUpdateSql(mtx, input, output, errs, log)
		default:

		}
	}

	data := &dpfm_api_output_formatter.Message{
		Header: header,
	}

	return data
}

func (c *DPFMAPICaller) headerCreateSql(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	subfuncSDC *sub_func_complementer.SDC,
	errs *[]error,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	if ctx == nil {
		ctx = context.Background()
	}
	sessionID := input.RuntimeSessionID

	dpfm_api_output_formatter.ConvertToHeader(input, subfuncSDC)

	headerData := subfuncSDC.Message.Header
	res, err := c.rmq.SessionKeepRequest(ctx, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": headerData, "function": "PointTransactionHeader", "runtime_session_id": sessionID})
	if err != nil {
		err = xerrors.Errorf("rmq error: %w", err)
		*errs = append(*errs, err)
		return nil
	}
	res.Success()
	if !checkResult(res) {
		output.SQLUpdateResult = getBoolPtr(false)
		output.SQLUpdateError = "Header Data cannot insert"
		return nil
	}

	if output.SQLUpdateResult == nil {
		output.SQLUpdateResult = getBoolPtr(true)
	}

	data, err := dpfm_api_output_formatter.ConvertToHeaderCreates(subfuncSDC)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) headerUpdateSql(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *dpfm_api_output_formatter.Header {
	header := input.Header
	headerData := dpfm_api_processing_formatter.ConvertToHeaderUpdates(header)

	sessionID := input.RuntimeSessionID
	if headerIsUpdate(headerData) {
		res, err := c.rmq.SessionKeepRequest(nil, c.conf.RMQ.QueueToSQL()[0], map[string]interface{}{"message": headerData, "function": "PointTransactionHeader", "runtime_session_id": sessionID})
		if err != nil {
			err = xerrors.Errorf("rmq error: %w", err)
			*errs = append(*errs, err)
			return nil
		}
		res.Success()
		if !checkResult(res) {
			output.SQLUpdateResult = getBoolPtr(false)
			output.SQLUpdateError = "Header Data cannot update"
			return nil
		}
	}

	if output.SQLUpdateResult == nil {
		output.SQLUpdateResult = getBoolPtr(true)
	}

	data, err := dpfm_api_output_formatter.ConvertToHeaderUpdates(header)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func headerIsUpdate(header *dpfm_api_processing_formatter.HeaderUpdates) bool {
	pointTransaction := header.PointTransaction

	return !(pointTransaction == 0)
}

func (c *DPFMAPICaller) CalculatePointTransaction(
	errs *[]error,
) *sub_func_complementer.CalculatePointTransactionQueryGets {
	pm := &sub_func_complementer.CalculatePointTransactionQueryGets{}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_number_range_latest_number_data
		WHERE (ServiceLabel, FieldNameWithNumberRange) = (?, ?);`, "POINT_TRANSACTION", "PointTransaction",
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	for i := 0; true; i++ {
		if !rows.Next() {
			if i == 0 {
				*errs = append(*errs, fmt.Errorf("'data_platform_number_range_latest_number_data'テーブルに対象のレコードが存在しません。"))
				return nil
			} else {
				break
			}
		}
		err = rows.Scan(
			&pm.NumberRangeID,
			&pm.ServiceLabel,
			&pm.FieldNameWithNumberRange,
			&pm.PointTransactionLatestNumber,
		)
		if err != nil {
			*errs = append(*errs, err)
			return nil
		}
	}

	return pm
}

func (c *DPFMAPICaller) UpdateLatestNumber(
	errs *[]error,
	pointTransactionIssuedID int,
) error {
	//rows, err := c.db.Query(
	//	`SELECT *
	//	FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_number_range_latest_number_data
	//	WHERE (ServiceLabel, FieldNameWithNumberRange) = (?, ?);`, "ORDERS", "PointTransaction",
	//)

	_, err := c.db.Exec(`
			UPDATE data_platform_number_range_latest_number_data SET LatestNumber=(?)
			WHERE (ServiceLabel, FieldNameWithNumberRange) = (?, ?);`,
		pointTransactionIssuedID,
		"POINT_TRANSACTION",
		"PointTransaction",
	)
	if err != nil {
		return xerrors.Errorf("'data_platform_number_range_latest_number_data'テーブルの更新に失敗しました。")
	}

	return nil
}
