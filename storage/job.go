// Copyright 2017 Frédéric Guillot. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package storage // import "miniflux.app/storage"

import (
	"fmt"

	"miniflux.app/model"
)

const maxParsingError = 3

// NewBatch returns a serie of jobs.
func (s *Storage) NewBatch(batchSize int) (jobs model.JobList, err error) {
	query := `
		WITH t AS (
		  SELECT
		  feed_id,
		  CASE
		    WHEN current_timestamp - max(published_at) < interval '9 days'
		      THEN interval '0 hours'
		    WHEN current_timestamp - max(published_at) < interval '22 days'
		      THEN interval '6 hours'
		    ELSE
		      interval '24 hours'
		  END
		  AS minimum_interval
		  FROM entries
		  GROUP BY feed_id
		)
		SELECT
			id,
			user_id
		FROM
			feeds
		JOIN
			t ON feeds.id = t.feed_id
		WHERE
			parsing_error_count < $1 AND disabled is false
			AND checked_at + minimum_interval <= current_timestamp
		ORDER BY checked_at ASC LIMIT %d
	`
	return s.fetchBatchRows(fmt.Sprintf(query, batchSize), maxParsingError)
}

// NewUserBatch returns a serie of jobs but only for a given user.
func (s *Storage) NewUserBatch(userID int64, batchSize int) (jobs model.JobList, err error) {
	// We do not take the error counter into consideration when the given
	// user refresh manually all his feeds to force a refresh.
	query := `
		SELECT
			id,
			user_id
		FROM
			feeds
		WHERE
			user_id=$1 AND disabled is false
		ORDER BY checked_at ASC LIMIT %d
	`
	return s.fetchBatchRows(fmt.Sprintf(query, batchSize), userID)
}

func (s *Storage) fetchBatchRows(query string, args ...interface{}) (jobs model.JobList, err error) {
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf(`store: unable to fetch batch of jobs: %v`, err)
	}
	defer rows.Close()

	for rows.Next() {
		var job model.Job
		if err := rows.Scan(&job.FeedID, &job.UserID); err != nil {
			return nil, fmt.Errorf(`store: unable to fetch job: %v`, err)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}
