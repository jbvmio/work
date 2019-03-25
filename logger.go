package work

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Field represents a key value pair for a Logger.
type Field zapcore.Field

// Logger interface for implementing logging.
type Logger interface {
	Debug(string, ...zapcore.Field)
}

type fieldPair map[string]interface{}

func newFieldMap() fieldPair {
	return make(map[string]interface{})
}

// DefaultLogger is a simple instance of a Logger that can be used for logging.
// For more advanced logging features, use your own logger.
type DefaultLogger struct {
	Logger *log.Logger
	lock   sync.RWMutex
}

// LogWith creates Ad-Hoc Fields for the DefaultLogger.
func LogWith(key string, value interface{}) zapcore.Field {
	switch value.(type) {
	case string:
		return zap.String(key, value.(string))
	case int:
		return zap.Int(key, value.(int))
	}
	return zap.Any(key, value)
}

// Debug prints a Debug message with entered fields.
func (l *DefaultLogger) Debug(message string, fields ...zapcore.Field) {
	l.Logger.Println(getMSG("[DEBUG]", message, fields))
}

func getMSG(msgType, message string, fields []zapcore.Field) string {
	currentMap := newFieldMap()
	message = msgType + message
	for _, f := range fields {
		switch f.Type {
		case 15:
			currentMap[f.Key] = f.String
		case 4:
			if f.Integer == 1 {
				currentMap[f.Key] = true
			}
			if f.Integer == 0 {
				currentMap[f.Key] = false
			}
		case 11, 12, 13, 14, 17, 18, 19, 20:
			currentMap[f.Key] = f.Integer
		default:
			currentMap[f.Key] = f.Interface
		}
	}
	for k, v := range currentMap {
		message += fmt.Sprintf(",%v=%v", k, v)
	}
	return message
}

/*
// Reset clears all internally added Fields.
func reset(fieldMap fieldPair) {
	fieldMap = newStringMap()
}

// Reset clears all internally added Fields.
func (l *DefaultLogger) Reset() {
	l.fields = newStringMap()
}
*/

// Prefix returns the Prefix for the DefaultLogger.
func (l *DefaultLogger) Prefix() string {
	return l.Logger.Prefix()
}

// SetPrefix sets the Prefix for the DefaultLogger.
func (l *DefaultLogger) SetPrefix(prefix string) {
	l.Logger.SetPrefix(prefix)
}

// NewDefaultLogger returns a default Logger that can be used for simple logging.
func NewDefaultLogger() *DefaultLogger {
	logger := log.New(os.Stdout, string(""), log.LstdFlags)
	return &DefaultLogger{
		Logger: logger,
	}
}

func newNoopLogger() *DefaultLogger {
	logger := log.New(ioutil.Discard, "", log.LstdFlags)
	return &DefaultLogger{
		Logger: logger,
	}
}
