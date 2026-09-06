@{OPTIMIZER_VERSION=8,FORCE_JOIN_ORDER=true}

WITH
latest_candidate_job_versions AS (
  SELECT
    i.job_id,
    MAX(i.revision_version) AS job_revision_version
  FROM benchmark.job@{FORCE_INDEX=`benchmark.job_by_workspace_type`} i
  WHERE i.workspace_id = @workspace_id
    AND i.type = @job_type
    AND i.job_id >= @job_id_start
    AND i.job_id < @job_id_end
  GROUP BY i.job_id
),

latest_candidate_jobs AS (
  SELECT
    i.job_id,
    i.revision_version AS job_revision_version,
    i.type AS job_type,
    i.workspace_id,
    i.payload_data AS job_payload
  FROM latest_candidate_job_versions latest
  JOIN benchmark.job@{FORCE_INDEX=_BASE_TABLE} i
    ON i.job_id = latest.job_id
   AND i.revision_version = latest.job_revision_version
   AND i.workspace_id = @workspace_id
   AND i.type = @job_type
),

produced_jobs AS (
  SELECT DISTINCT
    produced.job_id,
    produced.job_revision_version
  FROM benchmark.artifact@{FORCE_INDEX=`benchmark.artifact_by_producer_job`} produced
  WHERE produced.job_id >= @job_id_start
    AND produced.job_id < @job_id_end
),

pending_jobs AS (
  SELECT
    candidate.*
  FROM latest_candidate_jobs candidate

  LEFT JOIN@{JOIN_METHOD=HASH_JOIN}
    produced_jobs produced
    ON produced.job_id = candidate.job_id
   AND produced.job_revision_version = candidate.job_revision_version

  WHERE produced.job_id IS NULL
),

candidate_graph_rows AS (
  SELECT
    pending.job_id,
    pending.job_revision_version,
    pending.job_type,
    pending.workspace_id,
    pending.job_payload,

    g.input_artifact_id,
    g.input_revision_version,
    g.input_strong,
    g.input_artifact_type,
    g.input_artifact_payload,
    g.invalid_flag_id
  FROM pending_jobs pending JOIN
  GRAPH_TABLE(
    pipeline_graph

    MATCH
      (job:Job {
        workspace_id: @workspace_id,
        type: @job_type
      })

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (job)
      @{JOIN_METHOD=APPLY_JOIN}
      <-[input_edge:InputTo]-
      @{JOIN_METHOD=APPLY_JOIN}
      (input_artifact:Artifact)

    OPTIONAL MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (input_artifact)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:HasFlag]->
      @{JOIN_METHOD=APPLY_JOIN}
      (flag:Flag {
        type: 'type.googleapis.com/benchmark.pipeline.flag.Invalid'
      })

    RETURN
      job.job_id AS job_id,
      job.revision_version AS job_revision_version,

      input_artifact.artifact_id AS input_artifact_id,
      input_artifact.revision_version AS input_revision_version,
      input_edge.strong AS input_strong,
      input_artifact.type AS input_artifact_type,
      input_artifact.payload_data AS input_artifact_payload,
      flag.flag_id AS invalid_flag_id
  ) g

    ON pending.job_id = g.job_id
   AND pending.job_revision_version = g.job_revision_version
),

disqualified_jobs AS (
  SELECT
    job_id,
    job_revision_version
  FROM candidate_graph_rows
  WHERE invalid_flag_id IS NOT NULL
  GROUP @{GROUP_METHOD=STREAM_GROUP} BY
    job_id,
    job_revision_version
)

SELECT
  cgr.job_id,
  cgr.job_revision_version,
  cgr.job_type,
  cgr.workspace_id,
  cgr.job_payload,

  cgr.input_artifact_id,
  cgr.input_revision_version,
  cgr.input_strong,
  cgr.input_artifact_type,
  cgr.input_artifact_payload
FROM candidate_graph_rows cgr

LEFT JOIN@{
  JOIN_METHOD=HASH_JOIN,
  HASH_JOIN_BUILD_SIDE=BUILD_RIGHT
} disqualified_jobs disqualified
  ON disqualified.job_id = cgr.job_id
 AND disqualified.job_revision_version = cgr.job_revision_version

WHERE disqualified.job_id IS NULL

ORDER BY
  cgr.job_id,
  cgr.job_revision_version DESC,
  cgr.input_strong DESC,
  cgr.input_artifact_id,
  cgr.input_revision_version DESC
