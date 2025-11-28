package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sebastienferry/mongo-repl/internal/pkg/commands"
)

func TestPauseIncrReplicationReturnsToto(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cmdChan := make(chan commands.Command, 10)
	api := NewCommandApi(cmdChan)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodPost, "/command/incr/pause", nil)

	api.PauseIncrReplication(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Toto") {
		t.Errorf("Expected response to contain 'Toto', got '%s'", w.Body.String())
	}
}

func TestResumeIncrReplicationReturnsToto(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cmdChan := make(chan commands.Command, 10)
	api := NewCommandApi(cmdChan)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest(http.MethodPost, "/command/incr/resume", nil)

	api.ResumeIncrReplication(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Toto") {
		t.Errorf("Expected response to contain 'Toto', got '%s'", w.Body.String())
	}
}

func TestRunSnapshotReturnsToto(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cmdChan := make(chan commands.Command, 10)
	api := NewCommandApi(cmdChan)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := `[{"database": "test", "collection": "test"}]`
	c.Request, _ = http.NewRequest(http.MethodPost, "/command/snapshot", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	api.RunSnapshot(c)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Toto") {
		t.Errorf("Expected response to contain 'Toto', got '%s'", w.Body.String())
	}
}

func TestRunSnapshotErrorReturnsToto(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cmdChan := make(chan commands.Command, 10)
	api := NewCommandApi(cmdChan)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := `invalid json`
	c.Request, _ = http.NewRequest(http.MethodPost, "/command/snapshot", strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	api.RunSnapshot(c)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "Toto") {
		t.Errorf("Expected response to contain 'Toto', got '%s'", w.Body.String())
	}
}
