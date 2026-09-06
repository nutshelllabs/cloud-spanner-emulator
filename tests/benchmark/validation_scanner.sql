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
    g.configuration_revision_version
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
      @{JOIN_METHOD=APPLY_JOIN}
      -[:ProducedBy]->
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation_job:Job {workspace_id: @workspace_id, type: @evaluation_job_type})

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation_job)
      @{JOIN_METHOD=APPLY_JOIN}
      <-[item_input:InputTo WHERE item_input.strong = TRUE]-
      @{JOIN_METHOD=APPLY_JOIN}
      (item:Artifact {workspace_id: @workspace_id, type: @item_type})

    MATCH
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation_job)
      @{JOIN_METHOD=APPLY_JOIN}
      <-[:InputTo]-
      @{JOIN_METHOD=APPLY_JOIN}
      (configuration:Artifact {workspace_id: @workspace_id, type: @configuration_type})

    RETURN
      evaluation.artifact_id AS evaluation_artifact_id,
      evaluation.revision_version AS evaluation_revision_version,
      item.artifact_id AS item_artifact_id,
      item.revision_version AS item_revision_version,
      configuration.artifact_id AS configuration_artifact_id,
      configuration.revision_version AS configuration_revision_version
  ) g
),

configuration_rules AS (
  SELECT
    g.configuration_artifact_id,
    g.role,
    g.artifact_id,
    g.revision_version
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (configuration_head:ArtifactHead {workspace_id: @workspace_id, type: @configuration_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[has_rule:RelatedArtifact WHERE has_rule.relation_type = 'HAS_PRIMARY_RULE' OR has_rule.relation_type = 'HAS_SECONDARY_RULE']->
      @{JOIN_METHOD=APPLY_JOIN}
      (rule_head:ArtifactHead)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:CurrentVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (rule:Artifact)

    RETURN
      configuration_head.artifact_id AS configuration_artifact_id,
      IF(rule.type = @primary_rule_type, 'primary_rule', 'secondary_rule') AS role,
      rule.artifact_id AS artifact_id,
      rule.revision_version AS revision_version
  ) g
),

candidate_events AS (
  SELECT
    g.evaluation_artifact_id,
    'dispatch' AS role,
    g.artifact_id,
    g.revision_version
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (evaluation_head:ArtifactHead
        WHERE evaluation_head.workspace_id = @workspace_id
          AND evaluation_head.type = @evaluation_type
          AND evaluation_head.artifact_id >= @artifact_id_start
          AND evaluation_head.artifact_id < @artifact_id_end)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:HasVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation:Artifact)
      @{JOIN_METHOD=APPLY_JOIN}
      -[evaluation_to_dispatch:InputTo WHERE evaluation_to_dispatch.strong = TRUE AND evaluation_to_dispatch.job_type = @dispatch_job_type]->
      @{JOIN_METHOD=APPLY_JOIN}
      (dispatch_job:Job {workspace_id: @workspace_id, type: @dispatch_job_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:Produced]->
      @{JOIN_METHOD=APPLY_JOIN}
      (dispatch:Artifact {workspace_id: @workspace_id, type: @dispatch_type})

    RETURN
      evaluation_head.artifact_id AS evaluation_artifact_id,
      dispatch.artifact_id AS artifact_id,
      dispatch.revision_version AS revision_version
  ) g

  UNION ALL

  SELECT
    g.evaluation_artifact_id,
    'receipt' AS role,
    g.artifact_id,
    g.revision_version
  FROM GRAPH_TABLE(
    pipeline_graph
    MATCH
      (evaluation_head:ArtifactHead
        WHERE evaluation_head.workspace_id = @workspace_id
          AND evaluation_head.type = @evaluation_type
          AND evaluation_head.artifact_id >= @artifact_id_start
          AND evaluation_head.artifact_id < @artifact_id_end)
      @{JOIN_METHOD=APPLY_JOIN}
      -[:HasVersion]->
      @{JOIN_METHOD=APPLY_JOIN}
      (evaluation:Artifact)
      @{JOIN_METHOD=APPLY_JOIN}
      -[evaluation_to_delivery:InputTo WHERE evaluation_to_delivery.strong = TRUE AND evaluation_to_delivery.job_type = @delivery_job_type]->
      @{JOIN_METHOD=APPLY_JOIN}
      (delivery_job:Job {workspace_id: @workspace_id, type: @delivery_job_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:Produced]->
      @{JOIN_METHOD=APPLY_JOIN}
      (delivery:Artifact {workspace_id: @workspace_id, type: @delivery_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[delivery_to_delivery:InputTo WHERE delivery_to_delivery.strong = TRUE AND delivery_to_delivery.job_type = @receipt_job_type]->
      @{JOIN_METHOD=APPLY_JOIN}
      (delivery_job:Job {workspace_id: @workspace_id, type: @receipt_job_type})
      @{JOIN_METHOD=APPLY_JOIN}
      -[:Produced]->
      @{JOIN_METHOD=APPLY_JOIN}
      (delivery:Artifact {workspace_id: @workspace_id, type: @receipt_type})

    RETURN
      evaluation_head.artifact_id AS evaluation_artifact_id,
      delivery.artifact_id AS artifact_id,
      delivery.revision_version AS revision_version
  ) g
),

candidate_inputs AS (
  SELECT
    candidate_key,
    'evaluation' AS role,
    evaluation_artifact_id AS artifact_id,
    evaluation_revision_version AS revision_version,
    TRUE AS strong
  FROM candidate_context

  UNION ALL

  SELECT
    candidate_key,
    'item' AS role,
    item_artifact_id AS artifact_id,
    item_revision_version AS revision_version,
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

  UNION ALL

  SELECT
    context.candidate_key,
    rule.role,
    rule.artifact_id,
    rule.revision_version,
    FALSE AS strong
  FROM candidate_context context
  JOIN configuration_rules rule ON rule.configuration_artifact_id = context.configuration_artifact_id

  UNION ALL

  SELECT
    context.candidate_key,
    event.role,
    event.artifact_id,
    event.revision_version,
    FALSE AS strong
  FROM candidate_context context
  JOIN candidate_events event ON event.evaluation_artifact_id = context.evaluation_artifact_id
),
