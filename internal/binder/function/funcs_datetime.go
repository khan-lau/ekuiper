// Copyright 2022-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"errors"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

var errTooManyArguments = errors.New("too many arguments")

type IntervalUnit string

// registerDateTimeFunc registers the date and time functions.
func registerDateTimeFunc() {
	builtins["now"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDateTime(false),
		val:   validFspArgs(),
	}
	builtins["current_timestamp"] = builtins["now"]
	builtins["local_time"] = builtins["now"]
	builtins["local_timestamp"] = builtins["now"]

	builtins["cur_date"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDate(),
		val:   ValidateNoArg,
	}
	builtins["current_date"] = builtins["cur_date"]

	builtins["cur_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec:  execGetCurrentDateTime(true),
		val:   validFspArgs(),
	}
	builtins["current_time"] = builtins["cur_time"]

	builtins["format_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			arg1 := cast.ToStringAlways(args[1])
			if s, err := cast.FormatTime(arg0, arg1); err == nil {
				return s, true
			} else {
				return err, false
			}
		},
		val: func(_ api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			if ast.IsNumericArg(args[1]) || ast.IsTimeArg(args[1]) || ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(1, "string")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["date_calc"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			arg1 := cast.ToStringAlways(args[1])

			unitSign := 1
			if len(arg1) > 0 && arg1[0] == '-' {
				unitSign = -1
				arg1 = arg1[1:]
			}

			unit, err := cast.InterfaceToDuration(cast.ToStringAlways(arg1))
			if err != nil {
				return err, false
			}

			t, err := cast.FormatTime(arg0.Add(unit*time.Duration(unitSign)), "yyyy-MM-dd HH:mm:ss")
			if err != nil {
				return err, false
			}

			return t, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}

			if !ast.IsStringArg(args[1]) {
				return ProduceErrInfo(1, "string")
			}
			return nil
		},
	}
	builtins["date_diff"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			arg1, err := cast.InterfaceToTime(args[1], "")
			if err != nil {
				return err, false
			}
			return arg1.Sub(arg0), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(2, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}

			if ast.IsNumericArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsBooleanArg(args[1]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_name"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Weekday().String(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_of_month"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Day(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day"] = builtins["day_of_month"]

	builtins["day_of_week"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.Weekday(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["day_of_year"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}
			return arg0.YearDay(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}

			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["from_days"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			days, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if days == 0 {
				return nil, true
			}

			t := time.Unix(0, 0).Add(time.Duration(days-1) * 24 * time.Hour)
			result, err := cast.FormatTime(t, "yyyy-MM-dd")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}
	builtins["from_unix_time"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			seconds, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if seconds == 0 {
				return nil, true
			}
			t := time.Unix(int64(seconds), 0).In(cast.GetConfiguredTimeZone())
			result, err := cast.FormatTime(t, "yyyy-MM-dd HH:mm:ss")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}

	builtins["hour"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Hour(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["last_day"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			year, month, _ := arg0.Date()
			lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC)
			result, err := cast.FormatTime(lastDay, "yyyy-MM-dd")
			if err != nil {
				return err, false
			}
			return result, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["microsecond"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Nanosecond() / 1000, true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["minute"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Minute(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["month"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return int(arg0.Month()), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}
	builtins["month_name"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Month().String(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(1, "datetime")
			}
			return nil
		},
	}
	builtins["second"] = builtinFunc{
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			arg0, err := cast.InterfaceToTime(args[0], "")
			if err != nil {
				return err, false
			}

			return arg0.Second(), true
		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "datetime")
			}
			return nil
		},
	}

	builtins["x_from_timestamp"] = builtinFunc{ // 自定义函数, 将10位精确到s的时间戳 或 13位精确到ms的时间戳转换位date类型
		fType: ast.FuncTypeScalar,
		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			seconds, err := cast.ToInt(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if seconds == 0 {
				return nil, true
			}

			var t time.Time
			if seconds <= 9999999999 { // 10位时间戳, 精确到s
				t = time.Unix(int64(seconds), 0).In(cast.GetConfiguredTimeZone())
			} else { // 13位时间戳, 精确到ms
				t = time.Unix(0, int64(seconds)).In(cast.GetConfiguredTimeZone())
			}

			result, err := cast.FormatTime(t, "yyyy-MM-dd HH:mm:ss")
			if err != nil {
				return err, false
			}
			return result, true

		},
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(1, len(args)); err != nil {
				return err
			}
			// if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
			if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) {
				return ProduceErrInfo(0, "int")
			}
			return nil
		},
		check: returnNilIfHasAnyNil,
	}

	// `自定义函数`: 判断时间是否在时间段内, 判断条件左开右开
	//
	// x_at_duration(timestamp, start, end)
	//  - timestamp: 时间戳, 10位精确到s的时间戳 或 13位精确到ms的时间戳
	//  - start: 开始时间 120000 int型 表示 12点整
	//  - end: 结束时间 123010 int型 表示 12点10分
	builtins["x_timestamp_in_duration"] = builtinFunc{
		fType: ast.FuncTypeScalar,

		exec: func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
			seconds, err := cast.ToInt64(args[0], cast.STRICT)
			if err != nil {
				return err, false
			}

			if seconds == 0 {
				return nil, true
			}

			var t time.Time
			if seconds <= 9999999999 { // 10位时间戳, 精确到s
				t = time.Unix(int64(seconds), 0).In(cast.GetConfiguredTimeZone())
			} else { // 13位时间戳, 精确到ms
				t = time.Unix(0, int64(seconds)).In(cast.GetConfiguredTimeZone())
			}

			paramStart, err := cast.ToInt(args[1], cast.STRICT)
			if err != nil {
				return err, false
			}
			startHour := paramStart / 10000
			startMinute := (paramStart % 10000) / 100
			startSecond := paramStart % 100

			if startHour < 0 || startHour > 23 || startMinute < 0 || startMinute > 59 || startSecond < 0 || startSecond > 59 {
				return errors.New("invalid start time"), false
			}

			paramEnd, err := cast.ToInt(args[2], cast.STRICT)
			if err != nil {
				return err, false
			}
			endHour := paramEnd / 10000
			endMinute := (paramEnd % 10000) / 100
			endSecond := paramEnd % 100

			if endHour < 0 || endHour > 23 || endMinute < 0 || endMinute > 59 || endSecond < 0 || endSecond > 59 {
				return errors.New("invalid end time"), false
			}

			localHour := t.Local().Hour()
			localMin := t.Local().Minute()
			localSec := t.Local().Second()

			startClock := startSecond + startMinute*60 + startHour*3600
			endClock := endSecond + endMinute*60 + endHour*3600
			currClock := localSec + localMin*60 + localHour*3600

			if endClock < startClock {
				return errors.New("end time must be later than start time"), false
			}

			result := false
			if currClock >= startClock && currClock <= endClock {
				result = true
			}

			return result, true
		},

		// 参数检查入口
		val: func(ctx api.FunctionContext, args []ast.Expr) error {
			if err := ValidateLen(3, len(args)); err != nil { // 检查参数数量
				return err
			}
			// if ast.IsNumericArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) { // 第一个参数 int64
			if ast.IsFloatArg(args[0]) || ast.IsStringArg(args[0]) || ast.IsBooleanArg(args[0]) { // 第一个参数 int64
				return ProduceErrInfo(0, "int64")
			}

			if ast.IsFloatArg(args[1]) || ast.IsStringArg(args[1]) || ast.IsBooleanArg(args[1]) { // 第2个参数 int "80000"
				// fmt.Printf("x_timestamp_in_duration args 1 : %#v", args[1])
				return ProduceErrInfo(1, "int, example - 80000") // 代表 08:00:00
			}

			if ast.IsFloatArg(args[2]) || ast.IsStringArg(args[2]) || ast.IsBooleanArg(args[2]) { // 第3个参数 int 83000
				// fmt.Printf("x_timestamp_in_duration args 2 : %#v", args[1])
				return ProduceErrInfo(2, "int, example - 83000") // 代表 08:30:00
			}
			return nil
		},

		check: returnNilIfHasAnyNil,
	}
}

func execGetCurrentDate() funcExe {
	return func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
		formatted, err := cast.FormatTime(time.Now(), "yyyy-MM-dd")
		if err != nil {
			return err, false
		}
		return formatted, true
	}
}

// validFspArgs returns a function that validates the 'fsp' arg.
func validFspArgs() funcVal {
	return func(ctx api.FunctionContext, args []ast.Expr) error {
		if len(args) < 1 {
			return nil
		}

		if len(args) > 1 {
			return errTooManyArguments
		}

		if !ast.IsIntegerArg(args[0]) {
			return ProduceErrInfo(0, "int")
		}

		return nil
	}
}

func execGetCurrentDateTime(timeOnly bool) funcExe {
	return func(ctx api.FunctionContext, args []interface{}) (interface{}, bool) {
		fsp := 0
		switch len(args) {
		case 0:
			fsp = 0
		default:
			fsp = args[0].(int)
		}
		formatted, err := getCurrentWithFsp(fsp, timeOnly)
		if err != nil {
			return err, false
		}
		return formatted, true
	}
}

// getCurrentWithFsp returns the current date/time with the specified number of fractional seconds precision.
func getCurrentWithFsp(fsp int, timeOnly bool) (string, error) {
	format := "yyyy-MM-dd HH:mm:ss"
	now := conf.GetNow().In(cast.GetConfiguredTimeZone())
	switch fsp {
	case 1:
		format += ".S"
	case 2:
		format += ".SS"
	case 3:
		format += ".SSS"
	case 4:
		format += ".SSSS"
	case 5:
		format += ".SSSSS"
	case 6:
		format += ".SSSSSS"
	default:
	}

	formatted, err := cast.FormatTime(now, format)
	if err != nil {
		return "", err
	}

	if timeOnly {
		return strings.SplitN(formatted, " ", 2)[1], nil
	}

	return formatted, nil
}
