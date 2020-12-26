package fs533lib

import "time"

const dateTimeFormat = "2006-01-02 15.04.05"

const longDateTimeFormat = "2006-01-02 15:04:05.000"

//ConvertCurrentTimeToString ... to convert time.Now to a specific datetime format
func ConvertCurrentTimeToString() string {
	return time.Now().Format(dateTimeFormat)
}

//ConvertTimeToLongString ... to convert time to a specific datetime format
func ConvertTimeToLongString(time time.Time) string {
	return time.Format(longDateTimeFormat)
}
