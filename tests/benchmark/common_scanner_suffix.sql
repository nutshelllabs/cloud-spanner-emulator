-- COMMON PART BEGINS HERE
candidate_summary AS (
  SELECT
    candidate_key,
    COUNTIF(artifact_id IS NOT NULL) AS input_artifact_count,
    SUM(revision_version) AS job_revision_version,
    COUNTIF(strong AND artifact_id IS NOT NULL) AS strong_input_count
  FROM candidate_inputs
  GROUP BY candidate_key
),

candidate_strong_inputs AS (
  SELECT
    ci.candidate_key,
    ci.artifact_id,
    ci.revision_version,
    cs.job_revision_version
  FROM candidate_inputs ci

  JOIN candidate_summary cs
    ON cs.candidate_key = ci.candidate_key

  WHERE ci.strong = TRUE
    AND ci.artifact_id IS NOT NULL
),

existing_used_strong_inputs AS (
  SELECT
    csi.candidate_key
  FROM candidate_strong_inputs csi

  JOIN@{JOIN_METHOD=APPLY_JOIN}
    benchmark.artifact_job@{FORCE_INDEX=_BASE_TABLE} existing_input
    ON existing_input.artifact_id = csi.artifact_id
   AND existing_input.revision_version = csi.revision_version
   AND existing_input.strong = TRUE
   AND existing_input.job_revision_version = csi.job_revision_version

  JOIN@{JOIN_METHOD=APPLY_JOIN}
    benchmark.job@{FORCE_INDEX=_BASE_TABLE} existing_job
    ON existing_job.job_id = existing_input.job_id
   AND existing_job.revision_version = existing_input.job_revision_version
   AND existing_job.workspace_id = @workspace_id
   AND existing_job.type = @job_type
   -- Only a job of this run's own binding suppresses this candidate. Sibling
   -- runs of a parameterized task share strong inputs, so without this
   -- they would disqualify each other.
   AND (
     (@task_binding_id IS NULL AND existing_job.task_binding_id IS NULL)
     OR existing_job.task_binding_id = @task_binding_id
   )
),

non_existing_candidates AS (
  SELECT
    cs.candidate_key,
    cs.input_artifact_count,
    cs.job_revision_version,
    cs.strong_input_count
  FROM candidate_summary cs

  LEFT JOIN existing_used_strong_inputs existing
    ON existing.candidate_key = cs.candidate_key

  WHERE existing.candidate_key IS NULL
),

candidate_artifacts AS (
  SELECT
    ci.candidate_key,
    nec.input_artifact_count,
    nec.job_revision_version,
    nec.strong_input_count,

    ci.role,
    ci.artifact_id,
    ci.revision_version,
    ci.strong
  FROM candidate_inputs ci

  JOIN non_existing_candidates nec
    ON nec.candidate_key = ci.candidate_key

  -- Only artifacts are hydrated and turned into edges; the revision-only row is
  -- already folded into job_revision_version.
  WHERE ci.artifact_id IS NOT NULL
),

artifact_rows AS (
  SELECT
    cp.candidate_key,
    'artifact' AS row_kind,

    cp.input_artifact_count,
    cp.job_revision_version,
    cp.strong_input_count,

    cp.role,
    cp.artifact_id,
    cp.revision_version,
    cp.strong,

    p.type AS artifact_type,
    p.payload_data AS artifact_payload,
    p.modified_timestamp AS artifact_modified_timestamp,

    CAST(NULL AS STRING) AS flag_id,
    CAST(NULL AS JSON) AS flag_payload

  FROM candidate_artifacts cp

  JOIN@{JOIN_METHOD=APPLY_JOIN}
    benchmark.artifact@{FORCE_INDEX=_BASE_TABLE} p
    ON p.artifact_id = cp.artifact_id
   AND p.revision_version = cp.revision_version
),

flag_rows AS (
  SELECT
    cp.candidate_key,
    'flag' AS row_kind,

    cp.input_artifact_count,
    cp.job_revision_version,
    cp.strong_input_count,

    cp.role,
    cp.artifact_id,
    cp.revision_version,
    cp.strong,

    CAST(NULL AS STRING) AS artifact_type,
    CAST(NULL AS JSON) AS artifact_payload,
    CAST(NULL AS TIMESTAMP) AS artifact_modified_timestamp,

    f.flag_id,
    f.payload_data AS flag_payload

  FROM candidate_artifacts cp

  JOIN@{JOIN_METHOD=APPLY_JOIN}
    benchmark.flag@{FORCE_INDEX=_BASE_TABLE} f
    ON f.artifact_id = cp.artifact_id
   AND f.revision_version = cp.revision_version
)

SELECT * FROM artifact_rows

UNION ALL

SELECT * FROM flag_rows

ORDER BY
  candidate_key,
  row_kind,
  role,
  artifact_id,
  revision_version DESC,
  flag_id
