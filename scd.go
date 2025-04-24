package mercor

import (
	"fmt"
	"reflect"

	"gorm.io/gorm"
)

type Reader interface {
	// dest should be a pointer
	Read(dest any, filters ...func(*gorm.DB) *gorm.DB) error
}

type Writer interface {
	// newRecord should be a pointer
	Write(newRecord any) error
}

type SCDer interface {
	Reader
	Writer
}

// SCDConfig holds the configuration for the Slowly Changing Dimension (SCD) pattern.
type SCDConfig struct {
	TableName string
	IDColumn  string
	// VersionColumn should be of int type
	VersionColumn string
}

func NewSCD(db *gorm.DB, config SCDConfig) SCDer {
	return &SCD{
		db,
		config,
	}
}

type SCD struct {
	db     *gorm.DB
	config SCDConfig
}

func (s *SCD) Read(dest any, filters ...func(*gorm.DB) *gorm.DB) error {
	idCol := s.config.IDColumn
	versionCol := s.config.VersionColumn
	tableName := s.config.TableName

	subQuery := s.db.Table(tableName)

	subQuery = subQuery.Select(
		idCol + ", MAX(" + versionCol + ") as max_version",
	).Group(idCol)

	mainQuery := s.db.Table(tableName)
	for _, filter := range filters {
		mainQuery = filter(mainQuery)
	}

	joinCondition := tableName + "." + idCol + " = latest." + idCol +
		" AND " + tableName + "." + versionCol + " = latest.max_version"

	return mainQuery.
		Joins("JOIN (?) AS latest ON "+joinCondition, subQuery).
		Find(dest).
		Error
}

func (s *SCD) Write(newRecord any) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		idField, err := getStructFieldName(tx, newRecord, s.config.IDColumn)
		if err != nil {
			return err
		}

		versionField, err := getStructFieldName(tx, newRecord, s.config.VersionColumn)
		if err != nil {
			return err
		}

		uidValue, err := getFieldValue(newRecord, idField)
		if err != nil {
			return err
		}

		var maxVersion int
		err = tx.Table(s.config.TableName).
			Select("COALESCE(MAX("+s.config.VersionColumn+"), 0)").
			Where(s.config.IDColumn+" = ?", uidValue).
			Scan(&maxVersion).Error

		if err != nil {
			return err
		}

		if err := setFieldValue(newRecord, versionField, maxVersion+1); err != nil {
			return err
		}

		return tx.Table(s.config.TableName).Create(newRecord).Error
	})
}

func getStructFieldName(db *gorm.DB, model any, columnName string) (string, error) {
	stmt := &gorm.Statement{DB: db}

	if err := stmt.Parse(model); err != nil {
		return "", err
	}

	for _, field := range stmt.Schema.Fields {
		if field.DBName == columnName {
			return field.Name, nil
		}
	}

	return "", fmt.Errorf("Column %s not found in model", columnName)
}

func getValue(data any, fieldName string) reflect.Value {
	val := reflect.ValueOf(data).Elem()
	return val.FieldByName(fieldName)
}

func getFieldValue(data any, fieldName string) (any, error) {
	field := getValue(data, fieldName)

	if !field.IsValid() {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}

	return field.Interface(), nil
}

func setFieldValue(data any, fieldName string, value any) error {
	field := getValue(data, fieldName)

	if !field.IsValid() {
		return fmt.Errorf("field %s not found", fieldName)
	}
	if !field.CanSet() {
		return fmt.Errorf("field %s cannot be set", fieldName)
	}

	field.Set(reflect.ValueOf(value))

	return nil
}
