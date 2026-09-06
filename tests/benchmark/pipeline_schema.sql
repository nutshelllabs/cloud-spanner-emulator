-- Synthetic artifact pipeline with generated JSON keys and versioned dependencies.
CREATE SCHEMA benchmark;

CREATE TABLE benchmark.workspace (
  workspace_id STRING(36) NOT NULL,
  payload_data JSON,
  workspace_definition_id STRING(MAX) AS (JSON_VALUE(payload_data, '$.definition_id')) STORED,
  modified_timestamp TIMESTAMP OPTIONS (
    allow_commit_timestamp = true
  ),
  created_image_tag STRING(MAX),
  modified_image_tag STRING(MAX),
) PRIMARY KEY(workspace_id);

CREATE TABLE benchmark.job (
  payload_data JSON,
  job_id STRING(36) NOT NULL AS (IFNULL(JSON_VALUE(payload_data, '$.id'), '')) STORED,
  revision_version INT64 NOT NULL AS (IFNULL(CAST(JSON_VALUE(payload_data, '$.version') AS INT64), 0)) STORED,
  workspace_id STRING(MAX) AS (JSON_VALUE(payload_data, '$.workspace_id')) STORED,
  type STRING(MAX) AS (JSON_VALUE(payload_data, '$.data."@type"')) STORED,
  task_binding_id STRING(MAX) AS (JSON_VALUE(payload_data, '$.binding_id')) STORED,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  created_image_tag STRING(MAX),
  modified_image_tag STRING(MAX),
) PRIMARY KEY(job_id, revision_version DESC);

CREATE INDEX benchmark.job_by_workspace_type ON benchmark.job(workspace_id, type, job_id, revision_version DESC);

CREATE INDEX benchmark.job_by_workspace_type_revision ON benchmark.job(workspace_id, type, revision_version DESC, job_id);

CREATE TABLE benchmark.artifact_head (
  artifact_id STRING(36) NOT NULL,
  workspace_id STRING(MAX) NOT NULL,
  type STRING(MAX) NOT NULL,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(artifact_id);

CREATE INDEX benchmark.artifact_head_by_workspace_type ON benchmark.artifact_head(workspace_id, type, artifact_id);

CREATE TABLE benchmark.latest_artifact (
  artifact_id STRING(36) NOT NULL,
  revision_version INT64 NOT NULL,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(artifact_id),
  INTERLEAVE IN PARENT benchmark.artifact_head ON DELETE CASCADE;

CREATE INDEX benchmark.latest_artifact_by_artifact_id_revision_version ON benchmark.latest_artifact(artifact_id, revision_version);

CREATE TABLE benchmark.artifact (
  payload_data JSON,
  artifact_id STRING(36) NOT NULL AS (IFNULL(JSON_VALUE(payload_data, '$.id'), '')) STORED,
  revision_version INT64 NOT NULL AS (IFNULL(CAST(JSON_VALUE(payload_data, '$.version') AS INT64), 0)) STORED,
  job_id STRING(36) AS (JSON_VALUE(payload_data, '$.job_id')) STORED,
  job_revision_version INT64 AS (CAST(JSON_VALUE(payload_data, '$.job_version') AS INT64)) STORED,
  workspace_id STRING(MAX) AS (JSON_VALUE(payload_data, '$.workspace_id')) STORED,
  type STRING(MAX) AS (JSON_VALUE(payload_data, '$.data."@type"')) STORED,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  created_image_tag STRING(MAX),
  modified_image_tag STRING(MAX),
  CONSTRAINT fk_artifact_producer_job FOREIGN KEY(job_id, job_revision_version) REFERENCES benchmark.job(job_id, revision_version) NOT ENFORCED,
) PRIMARY KEY(artifact_id, revision_version DESC),
  INTERLEAVE IN PARENT benchmark.artifact_head ON DELETE NO ACTION;

ALTER TABLE benchmark.latest_artifact ADD CONSTRAINT fk_latest_artifact_artifact FOREIGN KEY(artifact_id, revision_version) REFERENCES benchmark.artifact(artifact_id, revision_version);

CREATE INDEX benchmark.artifact_by_producer_job ON benchmark.artifact(job_id, job_revision_version, artifact_id, revision_version DESC);

CREATE INDEX benchmark.artifact_by_workspace_type ON benchmark.artifact(workspace_id, type, artifact_id, revision_version DESC);

CREATE TABLE benchmark.flag (
  payload_data JSON,
  artifact_id STRING(36) NOT NULL AS (IFNULL(JSON_VALUE(payload_data, '$.artifact_id'), '')) STORED,
  revision_version INT64 NOT NULL AS (IFNULL(CAST(JSON_VALUE(payload_data, '$.version') AS INT64), 0)) STORED,
  flag_id STRING(36) NOT NULL AS (IFNULL(JSON_VALUE(payload_data, '$.flag_id'), '')) STORED,
  workspace_id STRING(MAX) AS (JSON_VALUE(payload_data, '$.workspace_id')) STORED,
  type STRING(MAX) AS (JSON_VALUE(payload_data, '$.data."@type"')) STORED,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  created_image_tag STRING(MAX),
  modified_image_tag STRING(MAX),
) PRIMARY KEY(artifact_id, revision_version DESC, flag_id),
  INTERLEAVE IN PARENT benchmark.artifact ON DELETE CASCADE;

CREATE INDEX benchmark.flag_by_workspace_type ON benchmark.flag(workspace_id, type, artifact_id, revision_version DESC, flag_id);

CREATE CHANGE STREAM PipelineStream
FOR benchmark.workspace, benchmark.job, benchmark.artifact, benchmark.flag
OPTIONS (
  retention_period = '7d',
  value_capture_type = 'NEW_ROW'
);

CREATE TABLE benchmark.artifact_job (
  artifact_id STRING(36) NOT NULL,
  revision_version INT64 NOT NULL,
  job_id STRING(36) NOT NULL,
  job_revision_version INT64 NOT NULL,
  strong BOOL NOT NULL DEFAULT (FALSE),
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
  job_type STRING(MAX),
  CONSTRAINT fk_artifact_job_job FOREIGN KEY(job_id, job_revision_version) REFERENCES benchmark.job(job_id, revision_version),
  CONSTRAINT fk_artifact_job_artifact FOREIGN KEY(artifact_id, revision_version) REFERENCES benchmark.artifact(artifact_id, revision_version),
) PRIMARY KEY(artifact_id, revision_version DESC, job_id, job_revision_version DESC),
  INTERLEAVE IN PARENT benchmark.artifact ON DELETE CASCADE;

CREATE INDEX benchmark.artifact_job_by_job ON benchmark.artifact_job(job_id, job_revision_version DESC, artifact_id, revision_version DESC) STORING (strong);

CREATE INDEX benchmark.artifact_job_by_artifact_version_job_type ON benchmark.artifact_job(artifact_id, revision_version DESC, job_type, job_id, job_revision_version DESC);

CREATE TABLE benchmark.artifact_relation (
  source_artifact_id STRING(36) NOT NULL,
  relation_type STRING(128) NOT NULL,
  dest_artifact_id STRING(36) NOT NULL,
  workspace_id STRING(MAX) NOT NULL,
  modified_timestamp TIMESTAMP NOT NULL OPTIONS (
    allow_commit_timestamp = true
  ),
) PRIMARY KEY(source_artifact_id, relation_type, dest_artifact_id);

CREATE INDEX benchmark.artifact_relation_by_dest ON benchmark.artifact_relation(dest_artifact_id, relation_type, source_artifact_id);

CREATE INDEX benchmark.artifact_relation_by_workspace_type_dest ON benchmark.artifact_relation(workspace_id, relation_type, dest_artifact_id, source_artifact_id);

CREATE INDEX benchmark.artifact_relation_by_workspace_type_source ON benchmark.artifact_relation(workspace_id, relation_type, source_artifact_id, dest_artifact_id);

CREATE VIEW benchmark.produced_by_edge_view SQL SECURITY INVOKER AS SELECT
    p.artifact_id,
    p.revision_version,

    p.job_id,
    p.job_revision_version,

    p.modified_timestamp
  FROM benchmark.artifact@{FORCE_INDEX=_BASE_TABLE} p
  WHERE p.job_id IS NOT NULL
  AND p.job_revision_version IS NOT NULL;

CREATE VIEW benchmark.produced_edge_view SQL SECURITY INVOKER AS SELECT DISTINCT
    p.job_id,
    p.job_revision_version,

    p.artifact_id,
    p.revision_version AS artifact_revision_version
  FROM benchmark.artifact@{FORCE_INDEX=`benchmark.artifact_by_producer_job`} p
  WHERE p.job_id IS NOT NULL
  AND p.job_revision_version IS NOT NULL;

CREATE OR REPLACE PROPERTY GRAPH pipeline_graph
  NODE TABLES(
    benchmark.flag AS Flag
      KEY(artifact_id, revision_version, flag_id)
      LABEL Flag PROPERTIES(
        created_image_tag,
        flag_id,
        modified_image_tag,
        modified_timestamp,
        artifact_id,
        payload_data,
        workspace_id,
        revision_version,
        type),

    benchmark.artifact AS Artifact
      KEY(artifact_id, revision_version)
      LABEL Artifact PROPERTIES(
        created_image_tag,
        modified_image_tag,
        modified_timestamp,
        artifact_id,
        payload_data,
        workspace_id,
        revision_version,
        job_id,
        job_revision_version,
        type),

    benchmark.artifact_head AS ArtifactHead
      KEY(artifact_id)
      LABEL ArtifactHead PROPERTIES(
        modified_timestamp,
        artifact_id,
        workspace_id,
        type),

    benchmark.job AS Job
      KEY(job_id, revision_version)
      LABEL Job PROPERTIES(
        created_image_tag,
        task_binding_id,
        modified_image_tag,
        modified_timestamp,
        payload_data,
        workspace_id,
        revision_version,
        job_id,
        type)
  )
  EDGE TABLES(
    benchmark.latest_artifact AS CurrentVersion
      KEY(artifact_id)
      SOURCE KEY(artifact_id) REFERENCES ArtifactHead(artifact_id)
      DESTINATION KEY(artifact_id, revision_version) REFERENCES Artifact(artifact_id, revision_version)
      LABEL CurrentVersion PROPERTIES(
        modified_timestamp,
        artifact_id,
        revision_version),

    benchmark.flag AS HasFlag
      KEY(artifact_id, revision_version, flag_id)
      SOURCE KEY(artifact_id, revision_version) REFERENCES Artifact(artifact_id, revision_version)
      DESTINATION KEY(flag_id, artifact_id, revision_version) REFERENCES Flag(flag_id, artifact_id, revision_version)
      LABEL HasFlag PROPERTIES(
        created_image_tag,
        flag_id,
        modified_image_tag,
        modified_timestamp,
        artifact_id,
        payload_data,
        workspace_id,
        revision_version,
        type),

    benchmark.artifact_job AS InputTo
      KEY(artifact_id, revision_version, job_id, job_revision_version)
      SOURCE KEY(artifact_id, revision_version) REFERENCES Artifact(artifact_id, revision_version)
      DESTINATION KEY(job_id, job_revision_version) REFERENCES Job(job_id, revision_version)
      LABEL InputTo PROPERTIES(
        modified_timestamp,
        artifact_id,
        revision_version,
        job_id,
        job_revision_version,
        job_type,
        strong),

    benchmark.produced_edge_view AS Produced
      KEY(job_id, job_revision_version, artifact_id, artifact_revision_version)
      SOURCE KEY(job_id, job_revision_version) REFERENCES Job(job_id, revision_version)
      DESTINATION KEY(artifact_id, artifact_revision_version) REFERENCES Artifact(artifact_id, revision_version)
      LABEL Produced PROPERTIES(
        artifact_id,
        artifact_revision_version,
        job_id,
        job_revision_version),

    benchmark.produced_by_edge_view AS ProducedBy
      KEY(artifact_id, revision_version, job_id, job_revision_version)
      SOURCE KEY(artifact_id, revision_version) REFERENCES Artifact(artifact_id, revision_version)
      DESTINATION KEY(job_id, job_revision_version) REFERENCES Job(job_id, revision_version)
      LABEL ProducedBy PROPERTIES(
        modified_timestamp,
        artifact_id,
        revision_version,
        job_id,
        job_revision_version),

    benchmark.artifact_relation AS RelatedArtifact
      KEY(source_artifact_id, relation_type, dest_artifact_id)
      SOURCE KEY(source_artifact_id) REFERENCES ArtifactHead(artifact_id)
      DESTINATION KEY(dest_artifact_id) REFERENCES ArtifactHead(artifact_id)
      LABEL RelatedArtifact PROPERTIES(
        dest_artifact_id,
        modified_timestamp,
        relation_type,
        workspace_id,
        source_artifact_id),

    benchmark.artifact AS HasVersion
      KEY(artifact_id, revision_version)
      SOURCE KEY(artifact_id)
        REFERENCES ArtifactHead(artifact_id)
      DESTINATION KEY(artifact_id, revision_version)
        REFERENCES Artifact(artifact_id, revision_version)
      LABEL HasVersion PROPERTIES(
        artifact_id,
        revision_version,
        workspace_id,
        type,
        modified_timestamp
      )
  );
