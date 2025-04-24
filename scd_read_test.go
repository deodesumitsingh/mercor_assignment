package mercor_test

import (
	"context"
	"deodesumitsingh/mercor"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gormPG "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type UserSCD struct {
	ID        int    `gorm:"primaryKey"`
	UserID    string `gorm:"column:user_id;uniqueIndex:uid_version"`
	Version   int    `gorm:"column:version;uniqueIndex:uid_version"`
	Name      string
	Email     string
	Status    string
	UpdatedAt time.Time
}

func SetupDB(t *testing.T) (*gorm.DB, func()) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(
		ctx,
		"postgres:15-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connection").WithOccurrence(2).WithStartupTimeout(5*time.Second)),
		testcontainers.WithEnv(map[string]string{"PGOPTIONS": "-c default_transaction_isolation=serializable"}),
	)
	assert.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx)
	assert.NoError(t, err)

	db, err := gorm.Open(gormPG.Open(connStr), &gorm.Config{})
	assert.NoError(t, err)

	assert.NoError(t, db.AutoMigrate(&UserSCD{}))

	return db, func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}
}

func TestSCD_Read(t *testing.T) {
	db, cleanup := SetupDB(t)
	t.Cleanup(cleanup)

	scd := mercor.NewSCD(db, mercor.SCDConfig{
		TableName:     "user_scds",
		IDColumn:      "user_id",
		VersionColumn: "version",
	})

	now := time.Now()
	testData := []UserSCD{
		{UserID: "user_1", Version: 1, Name: "Alice", Status: "active", UpdatedAt: now},
		{UserID: "user_1", Version: 2, Name: "Alice", Status: "inactive", UpdatedAt: now.Add(1 * time.Hour)},
		{UserID: "user_2", Version: 1, Name: "Bob", Status: "active", UpdatedAt: now},
		{UserID: "user_2", Version: 2, Name: "BobKiller", Status: "active", UpdatedAt: now.Add(2 * time.Hour)},
		{UserID: "user_3", Version: 1, Name: "Charlie", Status: "active", UpdatedAt: now},
	}

	for _, u := range testData {
		assert.NoError(t, db.Create(&u).Error)
	}

	t.Run("Filter by active status", func(t *testing.T) {
		var result []UserSCD
		err := scd.Read(&result, func(d *gorm.DB) *gorm.DB {
			return d.Where("status = ?", "active")
		})
		assert.NoError(t, err)

		assert.Len(t, result, 2, "Should return 2 active records")

		expected := map[string]struct {
			Version int
			Name    string
		}{
			"user_2": {Version: 2, Name: "BobKiller"},
			"user_3": {Version: 1, Name: "Charlie"},
		}

		for _, res := range result {
			exp, exists := expected[res.UserID]
			assert.True(t, exists, "Unexpected UserID in results")
			assert.Equal(t, exp.Version, res.Version)
			assert.Equal(t, exp.Name, res.Name)
		}
	})

	t.Run("Filter by inactive status", func(t *testing.T) {
		var result []UserSCD
		err := scd.Read(&result, func(d *gorm.DB) *gorm.DB {
			return d.Where("status = ?", "inactive")
		})
		assert.NoError(t, err)

		assert.Len(t, result, 1, "Should return 1 active records")

		expected := map[string]struct {
			Version int
			Name    string
		}{
			"user_1": {Version: 2, Name: "Alice"},
		}

		for _, res := range result {
			exp, exists := expected[res.UserID]
			assert.True(t, exists, "Unexpected UserID in results")
			assert.Equal(t, exp.Version, res.Version)
			assert.Equal(t, exp.Name, res.Name)
		}
	})

	t.Run("Filter by name", func(t *testing.T) {
		var result []UserSCD
		err := scd.Read(&result, func(d *gorm.DB) *gorm.DB {
			return d.Where("name = ?", "Bob")
		})
		assert.NoError(t, err)

		assert.Len(t, result, 0, "Should return no record")
	})

	t.Run("Filter by time", func(t *testing.T) {
		var result []UserSCD

		err := scd.Read(&result, func(d *gorm.DB) *gorm.DB {
			return d.Where("updated_at > ?", now.Add(30*time.Minute))
		})
		assert.NoError(t, err)

		expected := map[string]bool{
			"user_1": true,
			"user_2": true,
		}

		for _, r := range result {
			assert.True(t, expected[r.UserID])
			assert.Equal(t, 2, r.Version)
		}
	})
}
