@{OPTIMIZER_VERSION=8,FORCE_JOIN_ORDER=true}

WITH
candidate_context AS (
  SELECT
    g.evaluation_artifact_id AS candidate_key,
    g.evaluation_artifact_id,
    g.evaluation_revision_version,
    g.item_artifact_id,
    g.item_revision_version,
    g.configuration_artifact_id,
    g.configuration_revision_version,
    ARRAY_AGG(
      IF(
        g.previous_dispatch_artifact_id IS NOT NULL,
        STRUCT(
          g.previous_dispatch_artifact_id AS artifact_id,
          g.previous_dispatch_revision_version AS revision_version
        ),
        NULL
      )
      IGNORE NULLS
      ORDER BY g.previous_dispatch_revision_version DESC,
        g.previous_dispatch_artifact_id DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS previous_dispatch
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (evaluation_head:ArtifactHead
        WHERE evaluation_head.workspace_id = @workspace_id
          AND evaluation_head.type = @evaluation_type
          AND evaluation_head.artifact_id >= @artifact_id_start
          AND evaluation_head.artifact_id < @artifact_id_end)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation:Artifact)

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:ProducedBy]->
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation_job:Job {workspace_id: @workspace_id, type: @evaluation_job_type})
      @{JOIN_METHOD=APPLY_JOIN}
      <-[item_evaluation:InputTo WHERE item_evaluation.strong = TRUE]-
      @{JOIN_METHOD=APPLY_JOIN}
      (item:Artifact {workspace_id: @workspace_id, type: @item_type})
      @{JOIN_METHOD=APPLY_JOIN}
      <-[:HasVersion]-
      @{JOIN_METHOD=APPLY_JOIN}
      (item_head:ArtifactHead)

    -- Proves this run's rule governs this item's group. The rule
    -- itself is a parameter, so nothing about it is returned.
    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (item_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[member_of:RelatedArtifact WHERE member_of.relation_type = 'MEMBER_OF']->
      @{JOIN_METHOD=APPLY_JOIN}
      (group_head:ArtifactHead {workspace_id: @workspace_id, type: @group_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[has_configuration:RelatedArtifact WHERE has_configuration.relation_type = 'HAS']->
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head:ArtifactHead {workspace_id: @workspace_id, type: @configuration_type})

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration:Artifact {workspace_id: @workspace_id, type: @configuration_type})

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[has_primary_rule:RelatedArtifact WHERE has_primary_rule.relation_type = 'HAS_PRIMARY_RULE']->
      @{JOIN_METHOD=APPLY_JOIN}
      (primary_rule_head:ArtifactHead
        WHERE primary_rule_head.workspace_id = @workspace_id
          AND primary_rule_head.type = @primary_rule_type
          AND primary_rule_head.artifact_id = @primary_rule_artifact_id)

    -- Any dispatch already sent for this item under this rule, across every
    -- evaluation version. The binding is what ties a job back to the rule.
    OPTIONAL MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation_head)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:HasVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (prior_evaluation:Artifact)
      @{JOIN_METHOD=APPLY_JOIN}
      -[evaluation_to_dispatch:InputTo WHERE evaluation_to_dispatch.strong = TRUE AND evaluation_to_dispatch.job_type = @dispatch_job_type]->
      @{JOIN_METHOD=APPLY_JOIN}
      (prior_dispatch_job:Job {
        workspace_id: @workspace_id,
        type: @dispatch_job_type,
        task_binding_id: @task_binding_id
      })
      @{JOIN_METHOD=APPLY_JOIN}
      -[:Produced]->
      @{JOIN_METHOD=APPLY_JOIN}
      (previous_dispatch:Artifact {workspace_id: @workspace_id, type: @dispatch_type})

    RETURN
      evaluation.artifact_id AS evaluation_artifact_id,
      evaluation.revision_version AS evaluation_revision_version,
      item.artifact_id AS item_artifact_id,
      item.revision_version AS item_revision_version,
      configuration.artifact_id AS configuration_artifact_id,
      configuration.revision_version AS configuration_revision_version,
      previous_dispatch.artifact_id AS previous_dispatch_artifact_id,
      previous_dispatch.revision_version AS previous_dispatch_revision_version
  ) g
  GROUP BY evaluation_artifact_id, evaluation_revision_version, item_artifact_id, item_revision_version, configuration_artifact_id, configuration_revision_version
),

candidate_inputs AS (
  SELECT candidate_key, 'evaluation' AS role, evaluation_artifact_id AS artifact_id, evaluation_revision_version AS revision_version, TRUE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'item' AS role, item_artifact_id AS artifact_id, item_revision_version AS revision_version, FALSE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'configuration' AS role, configuration_artifact_id AS artifact_id, configuration_revision_version AS revision_version, FALSE AS strong FROM candidate_context
  UNION ALL
  SELECT candidate_key, 'previous_dispatch' AS role, previous_dispatch.artifact_id, previous_dispatch.revision_version, FALSE AS strong
  FROM candidate_context
  WHERE previous_dispatch.artifact_id IS NOT NULL
),
