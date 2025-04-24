package mercor_test

import (
	"deodesumitsingh/mercor"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSCD_Write(t *testing.T) {
	db, cleanup := SetupDB(t)
	t.Cleanup(cleanup)

	scd := mercor.NewSCD(db, mercor.SCDConfig{
		TableName:     "user_scds",
		IDColumn:      "user_id",
		VersionColumn: "version",
	})

	t.Run("Initial version creation", func(t *testing.T) {
		user := &UserSCD{
			UserID: "test_123",
			Name:   "Test",
			Email:  "test@test.com",
			Status: "active",
		}
		err := scd.Write(user)
		assert.NoError(t, err)

		assert.Equal(t, 1, user.Version)
	})

	t.Run("Data change creates new version", func(t *testing.T) {
		// Initial version
		user := &UserSCD{
			UserID: "test_789",
			Name:   "test",
			Email:  "test@example.com",
			Status: "active",
		}
		assert.NoError(t, scd.Write(user))

		// Updated version
		updatedUser := &UserSCD{
			UserID: "test_789",
			Name:   "Test Updated",
			Email:  "test@exmaple.com",
			Status: "active",
		}
		assert.NoError(t, scd.Write(updatedUser))

		assert.Equal(t, 2, updatedUser.Version)
	})

}
