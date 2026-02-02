package aap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	// apiVersion is the AAP API version path
	apiVersion = "api/v2"
)

// Client provides an HTTP client for interacting with AAP (Ansible Automation Platform) REST API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// NewClient creates a new AAP API client.
func NewClient(baseURL, token string) *Client {
	return &Client{
		baseURL: baseURL,
		token:   token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// LaunchJobTemplateRequest contains parameters for launching a job template.
type LaunchJobTemplateRequest struct {
	TemplateName string
	ExtraVars    map[string]interface{}
}

// LaunchJobTemplateResponse contains the response from launching a job template.
type LaunchJobTemplateResponse struct {
	JobID int `json:"id"`
}

// LaunchWorkflowTemplateRequest contains parameters for launching a workflow template.
type LaunchWorkflowTemplateRequest struct {
	TemplateName string
	ExtraVars    map[string]interface{}
}

// LaunchWorkflowTemplateResponse contains the response from launching a workflow template.
type LaunchWorkflowTemplateResponse struct {
	JobID int `json:"id"`
}

// Job represents an AAP job with status information.
type Job struct {
	ID              int                    `json:"id"`
	Status          string                 `json:"status"`
	Started         time.Time              `json:"started"`
	Finished        time.Time              `json:"finished"`
	ExtraVars       map[string]interface{} `json:"extra_vars"`
	ResultTraceback string                 `json:"result_traceback"`
}

// TemplateType represents the type of AAP template.
type TemplateType string

const (
	TemplateTypeJob      TemplateType = "job_template"
	TemplateTypeWorkflow TemplateType = "workflow_job_template"
)

// LaunchJobTemplate launches a job template and returns the job ID.
func (c *Client) LaunchJobTemplate(ctx context.Context, req LaunchJobTemplateRequest) (*LaunchJobTemplateResponse, error) {
	url := fmt.Sprintf("%s/%s/job_templates/%s/launch/", c.baseURL, apiVersion, req.TemplateName)

	payload := map[string]interface{}{
		"extra_vars": req.ExtraVars,
	}

	resp, err := c.doRequest(ctx, http.MethodPost, url, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to launch job template: %w", err)
	}

	var launchResp LaunchJobTemplateResponse
	if err := json.Unmarshal(resp, &launchResp); err != nil {
		return nil, fmt.Errorf("failed to parse launch response: %w", err)
	}

	return &launchResp, nil
}

// LaunchWorkflowTemplate launches a workflow template and returns the job ID.
func (c *Client) LaunchWorkflowTemplate(ctx context.Context, req LaunchWorkflowTemplateRequest) (*LaunchWorkflowTemplateResponse, error) {
	url := fmt.Sprintf("%s/%s/workflow_job_templates/%s/launch/", c.baseURL, apiVersion, req.TemplateName)

	payload := map[string]interface{}{
		"extra_vars": req.ExtraVars,
	}

	resp, err := c.doRequest(ctx, http.MethodPost, url, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to launch workflow template: %w", err)
	}

	var launchResp LaunchWorkflowTemplateResponse
	if err := json.Unmarshal(resp, &launchResp); err != nil {
		return nil, fmt.Errorf("failed to parse launch response: %w", err)
	}

	return &launchResp, nil
}

// GetJob retrieves job status by job ID.
func (c *Client) GetJob(ctx context.Context, jobID int) (*Job, error) {
	url := fmt.Sprintf("%s/%s/jobs/%d/", c.baseURL, apiVersion, jobID)

	resp, err := c.doRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	var job Job
	if err := json.Unmarshal(resp, &job); err != nil {
		return nil, fmt.Errorf("failed to parse job response: %w", err)
	}

	return &job, nil
}

// GetTemplateType queries AAP to determine if a template is a job_template or workflow_job_template.
func (c *Client) GetTemplateType(ctx context.Context, templateName string) (TemplateType, error) {
	// Try job template first
	jobURL := fmt.Sprintf("%s/%s/job_templates/%s/", c.baseURL, apiVersion, templateName)
	_, err := c.doRequest(ctx, http.MethodGet, jobURL, nil)
	if err == nil {
		return TemplateTypeJob, nil
	}

	// Try workflow template
	workflowURL := fmt.Sprintf("%s/%s/workflow_job_templates/%s/", c.baseURL, apiVersion, templateName)
	_, err = c.doRequest(ctx, http.MethodGet, workflowURL, nil)
	if err == nil {
		return TemplateTypeWorkflow, nil
	}

	return "", fmt.Errorf("template %s not found as job_template or workflow_job_template", templateName)
}

// doRequest performs an HTTP request with authentication and returns the response body.
func (c *Client) doRequest(ctx context.Context, method, url string, payload interface{}) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		jsonData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload: %w", err)
		}
		body = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("received non-success status code %d: %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}
