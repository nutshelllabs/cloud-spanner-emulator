@{OPTIMIZER_VERSION=8,FORCE_JOIN_ORDER=true}

WITH
candidate_context AS (
  SELECT
    g.batch_artifact_id AS candidate_key,
    g.batch_artifact_id,
    g.batch_revision_version,
    g.group_artifact_id,
    g.group_revision_version,
    g.configuration_artifact_id,
    g.configuration_revision_version
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (batch_head:ArtifactHead
        WHERE batch_head.workspace_id = @workspace_id
          AND batch_head.type = @batch_type
          AND batch_head.artifact_id >= @artifact_id_start
          AND batch_head.artifact_id < @artifact_id_end)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (batch:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (batch_head)
      @{JOIN_METHOD=APPLY_JOIN}
      <-[has_batch:RelatedArtifact WHERE has_batch.relation_type = 'HAS_BATCH']-
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head:ArtifactHead {workspace_id: @workspace_id, type: @configuration_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head)
      @{JOIN_METHOD=APPLY_JOIN}
      <-[has_configuration:RelatedArtifact WHERE has_configuration.relation_type = 'HAS']-
      @{JOIN_METHOD=APPLY_JOIN}
      (group_head:ArtifactHead {workspace_id: @workspace_id, type: @group_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (group_node:Artifact)

    RETURN
      batch.artifact_id AS batch_artifact_id,
      batch.revision_version AS batch_revision_version,
      group_node.artifact_id AS group_artifact_id,
      group_node.revision_version AS group_revision_version,
      configuration.artifact_id AS configuration_artifact_id,
      configuration.revision_version AS configuration_revision_version
  ) g
),

-- Every current validation check and its evaluation, rooted back at the
-- batch. Complete checks contribute both artifacts to membership;
-- incomplete checks contribute only revision version movement.
candidate_checks AS (
  SELECT
    batch_artifact_id,
    evaluation_artifact_id,
    evaluation_revision_version,
    check_artifact_id,
    check_revision_version,
    IFNULL(complete, 'false') = 'true'
      AND IFNULL(selected, 'false') = 'true'
      AND incomplete_flag_id IS NULL AS included
  FROM (
    SELECT
      g.batch_artifact_id,
      g.evaluation_artifact_id,
      g.evaluation_revision_version,
      g.check_artifact_id,
      g.check_revision_version,
      g.complete,
      g.selected,
      g.incomplete_flag_id
    FROM GRAPH_TABLE(
      pipeline_graph
      MATCH
        (check_head:ArtifactHead {workspace_id: @workspace_id, type: @validation_type})
        @{JOIN_METHOD=APPLY_JOIN}
        -[:CurrentVersion]->
        @{JOIN_METHOD=APPLY_JOIN}
        (check:Artifact)

      OPTIONAL MATCH
        @{JOIN_METHOD=APPLY_JOIN}
        (check)
        @{JOIN_METHOD=APPLY_JOIN}
        -[:HasFlag]->
        @{JOIN_METHOD=APPLY_JOIN}
        (incomplete_flag:Flag {type: @incomplete_validation_flag_type})

      MATCH
        @{JOIN_METHOD=APPLY_JOIN}
        (check)
        @{JOIN_METHOD=APPLY_JOIN}
        -[:ProducedBy]->
        @{JOIN_METHOD=APPLY_JOIN}
        (check_job:Job {workspace_id: @workspace_id, type: @validation_job_type})
        @{JOIN_METHOD=APPLY_JOIN}
        <-[check_evaluation:InputTo WHERE check_evaluation.strong = TRUE]-
        @{JOIN_METHOD=APPLY_JOIN}
        (evaluation_version:Artifact {workspace_id: @workspace_id, type: @evaluation_type})
        @{JOIN_METHOD=APPLY_JOIN}
        <-[:HasVersion]-
        @{JOIN_METHOD=APPLY_JOIN}
        (evaluation_head:ArtifactHead)
        @{JOIN_METHOD=APPLY_JOIN}
        -[:CurrentVersion]->
        @{JOIN_METHOD=APPLY_JOIN}
        (evaluation:Artifact)
        @{JOIN_METHOD=APPLY_JOIN}
        -[:ProducedBy]->
        @{JOIN_METHOD=APPLY_JOIN}
        (evaluation_job:Job {workspace_id: @workspace_id, type: @evaluation_job_type})
        @{JOIN_METHOD=APPLY_JOIN}
        <-[:InputTo]-
        @{JOIN_METHOD=APPLY_JOIN}
        (evaluated_batch:Artifact {workspace_id: @workspace_id, type: @batch_type})
        @{JOIN_METHOD=APPLY_JOIN}
        <-[:HasVersion]-
        @{JOIN_METHOD=APPLY_JOIN}
        (batch_head:ArtifactHead
          WHERE batch_head.artifact_id >= @artifact_id_start
            AND batch_head.artifact_id < @artifact_id_end)

      RETURN
        batch_head.artifact_id AS batch_artifact_id,
        evaluation.artifact_id AS evaluation_artifact_id,
        evaluation.revision_version AS evaluation_revision_version,
        check.artifact_id AS check_artifact_id,
        check.revision_version AS check_revision_version,
        JSON_VALUE(check.payload_data, '$.data.complete') AS complete,
        JSON_VALUE(evaluation.payload_data, '$.data.selected') AS selected,
        incomplete_flag.flag_id AS incomplete_flag_id
    ) g
  )
),

candidate_inputs AS (
  SELECT
    candidate_key,
    'batch' AS role,
    batch_artifact_id AS artifact_id,
    batch_revision_version AS revision_version,
    TRUE AS strong
  FROM candidate_context

  UNION ALL

  SELECT
    candidate_key,
    'group' AS role,
    group_artifact_id AS artifact_id,
    group_revision_version AS revision_version,
    FALSE AS strong
  FROM candidate_context

  UNION ALL

  SELECT
    candidate_key,
    'configuration' AS role,
    configuration_artifact_id AS artifact_id,
    configuration_revision_version AS revision_version,
    FALSE AS strong
  FROM candidate_context

  -- These edges are the aggregate's membership record.
  UNION ALL

  SELECT
    context.candidate_key,
    'evaluation' AS role,
    evaluation.evaluation_artifact_id AS artifact_id,
    evaluation.evaluation_revision_version AS revision_version,
    FALSE AS strong
  FROM candidate_context context
  JOIN candidate_checks evaluation ON evaluation.batch_artifact_id = context.batch_artifact_id
  WHERE evaluation.included

  UNION ALL

  SELECT context.candidate_key, 'validation' AS role,
    evaluation.check_artifact_id AS artifact_id, evaluation.check_revision_version AS revision_version, FALSE AS strong
  FROM candidate_context context
  JOIN candidate_checks evaluation ON evaluation.batch_artifact_id = context.batch_artifact_id
  WHERE evaluation.included

  -- Everything else moves the version as one row, outside the lineage.
  UNION ALL

  SELECT
    context.candidate_key,
    'revision-only' AS role,
    CAST(NULL AS STRING) AS artifact_id,
    SUM(evaluation.evaluation_revision_version + evaluation.check_revision_version) AS revision_version,
    FALSE AS strong
  FROM candidate_context context
  JOIN candidate_checks evaluation
    ON evaluation.batch_artifact_id = context.batch_artifact_id
  WHERE NOT evaluation.included
  GROUP BY context.candidate_key
),
