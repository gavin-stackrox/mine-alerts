package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

const (
	from        = "2024-02-18"
	to          = "2024-03-03"
	alert_count = 3
)

type Job struct {
	Id         string
	Started_at string
	Name       string
	Outcome    string
	Logs_link  string
}

type Test struct {
	BuildId   string
	JobName   string
	Classname string
	Name      string
}

func main() {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "acs-san-stackroxci")
	if err != nil {
		log.Fatalf("NewClient failed: %v", err)
	}

	q := client.Query(`
		SELECT id, string(started_at) as started_at, name, IFNULL(outcome, "null") as outcome, logs_link
		FROM acs-san-stackroxci.ci_metrics.stackrox_jobs__extended_view
		WHERE starts_with(name, "branch-ci-stackrox-stackrox-master-") AND started_at >= timestamp(@from) AND started_at <= timestamp(@to)
		ORDER BY started_at ASC
	`)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "from", Value: from},
		{Name: "to", Value: to},
	}
	it, err := q.Read(ctx)
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}

	jobs_by_name := make(map[string][]*Job)

	for {
		var job Job
		err := it.Next(&job)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Next failed: %v", err)
		}
		jobs_by_name[job.Name] = append(jobs_by_name[job.Name], &job)
	}

	q = client.Query(`
		SELECT BuildId, JobName, Classname, Name
		FROM acs-san-stackroxci.ci_metrics.stackrox_tests__extended_view
		WHERE Status = "failed" AND Timestamp >= timestamp(@from) AND Timestamp <= timestamp(@to)
	`)
	q.Parameters = []bigquery.QueryParameter{
		{Name: "from", Value: from},
		{Name: "to", Value: to},
	}
	it, err = q.Read(ctx)
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}

	failed_tests_by_job_name := make(map[string][]*Test)

	for {
		var test Test
		err := it.Next(&test)
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Next failed: %v", err)
		}
		failed_tests_by_job_name[test.JobName] = append(failed_tests_by_job_name[test.JobName], &test)
	}

	for name, jobs := range jobs_by_name {
		process_job(name, jobs, failed_tests_by_job_name[name])
	}
}

func process_job(name string, jobs []*Job, tests []*Test) {
	contiguous_failures := [][]*Job{}

	for idx, job := range jobs {
		if job.Outcome != "failed" {
			continue
		}
		if idx == 0 || jobs[idx-1].Outcome != "failed" {
			// a possible start of a failure batch
			contiguous_failures = append(contiguous_failures, []*Job{job})
		} else {
			// a continuation of a previous failure batch
			contiguous_failures[len(contiguous_failures)-1] = append(contiguous_failures[len(contiguous_failures)-1], job)
		}
	}

	fmt.Printf("%s, %d jobs\n", name, len(jobs))

	for _, batch := range contiguous_failures {
		process_job_batch(batch, tests)
	}
}

func process_job_batch(jobs []*Job, tests []*Test) {
	if len(jobs) < alert_count {
		return
	}

	fmt.Printf("\tpotential batch for alert: %s-%s, %d jobs\n",
		jobs[0].Started_at, jobs[len(jobs)-1].Started_at, len(jobs))

	for _, job := range jobs {
		fmt.Printf("\t\t%s\n", job.Logs_link)

		for _, test := range tests {
			if test.BuildId != job.Id {
				continue
			}
			fmt.Printf("\t\t\t%s, %s\n", test.Classname, test.Name)
		}
	}
}
